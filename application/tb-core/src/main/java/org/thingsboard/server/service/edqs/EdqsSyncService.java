/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.service.edqs;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.thingsboard.server.common.data.AttributeScope;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.ObjectType;
import org.thingsboard.server.common.data.edqs.AttributeKv;
import org.thingsboard.server.common.data.edqs.EdqsEventType;
import org.thingsboard.server.common.data.edqs.EdqsObject;
import org.thingsboard.server.common.data.edqs.Entity;
import org.thingsboard.server.common.data.edqs.LatestTsKv;
import org.thingsboard.server.common.data.edqs.fields.EntityFields;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.EntityIdFactory;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.page.PageDataIterable;
import org.thingsboard.server.common.data.relation.RelationTypeGroup;
import org.thingsboard.server.dao.Dao;
import org.thingsboard.server.dao.attributes.AttributesDao;
import org.thingsboard.server.dao.dictionary.KeyDictionaryDao;
import org.thingsboard.server.dao.entity.EntityDaoRegistry;
import org.thingsboard.server.dao.model.sql.AttributeKvEntity;
import org.thingsboard.server.dao.model.sql.RelationEntity;
import org.thingsboard.server.dao.model.sqlts.dictionary.KeyDictionaryEntry;
import org.thingsboard.server.dao.model.sqlts.latest.TsKvLatestEntity;
import org.thingsboard.server.dao.sql.relation.RelationRepository;
import org.thingsboard.server.dao.sqlts.latest.TsKvLatestRepository;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.thingsboard.server.common.data.ObjectType.ATTRIBUTE_KV;
import static org.thingsboard.server.common.data.ObjectType.LATEST_TS_KV;
import static org.thingsboard.server.common.data.ObjectType.RELATION;
import static org.thingsboard.server.common.data.ObjectType.edqsTenantTypes;

/**
 * EDQS 全量同步抽象基类：从关系型数据库<strong>分批扫描</strong>存量数据，
 * 转成 UPDATED 事件，经 {@link DefaultEdqsService} 写入 EDQS 事件队列。
 * <p>
 * 调用方是 {@link DefaultEdqsService#processSystemMsg}：集群内抢到
 * {@code edqs_sync} 锁的 Core 节点在状态为非 FINISHED/FAILED 时执行 {@link #sync()}。
 * 同步本身只负责「灌事件」；是否打开查询 API、同步状态机落库，由
 * {@link DefaultEdqsService} 负责。
 * <p>
 * <b>同步顺序（有依赖，不可随意调换）：</b>
 * <ol>
 *   <li>{@link #syncTenantEntities()}：按 {@link ObjectType#edqsTenantTypes} 扫实体，
 *       并填充 {@link #entityInfoMap}（后续关系/属性/时序靠它反查租户与类型）；</li>
 *   <li>{@link #syncRelations()}：仅 COMMON 关系组；from 必须已在 entityInfoMap；</li>
 *   <li>{@link #loadKeyDictionary()}：预加载 keyId → 字符串，供属性/最新时序解析；</li>
 *   <li>{@link #syncAttributes()}：全表属性批次；</li>
 *   <li>{@link #syncLatestTimeseries()}：最新时序批次。</li>
 * </ol>
 * <p>
 * <b>子类职责：</b>仅实现 {@link #isSyncNeeded()}，根据存储介质判断「冷启动是否还要灌数」：
 * <ul>
 *   <li>{@link KafkaEdqsSyncService}：events Topic 全部分区为空 → 需要；</li>
 *   <li>{@link LocalEdqsSyncService}：本地 RocksDB 为新建空库 → 需要。</li>
 * </ul>
 * 即使 {@link #isSyncNeeded()} 为 false，若系统属性 {@code edqsSyncState} 未 FINISHED，
 * {@link DefaultEdqsService} 仍可能再次触发 {@link #sync()}（以状态机为准）。
 * <p>
 * 批次大小由配置 {@code queue.edqs.sync.entity_batch_size} /
 * {@code queue.edqs.sync.ts_batch_size} 控制；单条失败只记日志，不中断整批。
 *
 * @see DefaultEdqsService
 * @see KafkaEdqsSyncService
 * @see LocalEdqsSyncService
 */
@Slf4j
public abstract class EdqsSyncService {

    /**
     * 实体与关系扫描的每批条数。
     * 配置项：{@code queue.edqs.sync.entity_batch_size}，默认 10000。
     */
    @Value("${queue.edqs.sync.entity_batch_size:10000}")
    private int entityBatchSize;
    /**
     * 属性与最新时序扫描的每批条数（通常行数更大，可与实体批次分开调）。
     * 配置项：{@code queue.edqs.sync.ts_batch_size}，默认 10000。
     */
    @Value("${queue.edqs.sync.ts_batch_size:10000}")
    private int tsBatchSize;
    /** 按 {@link EntityType} 取对应 Dao，用于 {@code findNextBatch} 游标扫描。 */
    @Autowired
    private EntityDaoRegistry entityDaoRegistry;
    @Autowired
    private AttributesDao attributesDao;
    /** 键字典：属性/时序在库中存 keyId，发往 EDQS 前需还原为字符串 key。 */
    @Autowired
    private KeyDictionaryDao keyDictionaryDao;
    @Autowired
    private RelationRepository relationRepository;
    @Autowired
    private TsKvLatestRepository tsKvLatestRepository;
    /**
     * 事件写出入口；延迟注入避免与 {@link DefaultEdqsService}（持有本抽象类子类）循环依赖。
     * 同步路径直接调 {@link DefaultEdqsService#processEvent}，不经 onUpdate 类型再过滤。
     */
    @Autowired
    @Lazy
    private DefaultEdqsService edqsService;

    /**
     * 实体 UUID → 类型与租户。
     * <p>
     * 在 {@link #syncTenantEntities()} 中填充；关系/属性/最新时序批次靠此映射
     * 补全 {@link TenantId} 与 {@link EntityType}（库表往往只存 entity UUID）。
     * 找不到映射的行会跳过（脏数据或实体类型不在 edqsTenantTypes）。
     */
    private final ConcurrentHashMap<UUID, EntityIdInfo> entityInfoMap = new ConcurrentHashMap<>();
    /**
     * 键字典缓存：keyId → 字符串 key。
     * {@link #loadKeyDictionary()} 预热，{@link #getStrKeyOrFetchFromDb} 未命中再回源。
     */
    private final ConcurrentHashMap<Integer, String> keys = new ConcurrentHashMap<>();

    /** 按 {@link ObjectType} 统计已处理条数，每 10000 条打一条 info 进度日志。 */
    private final Map<ObjectType, AtomicInteger> counters = new ConcurrentHashMap<>();

    /**
     * 当前部署介质是否「看起来」还需要做一次冷启动全量灌数。
     * <p>
     * 由子类在构造或首次判断时根据 Kafka topic / RocksDB 是否为空实现。
     * 结果供 {@link DefaultEdqsService#onStartUp} 与状态机一起决定是否发起同步。
     *
     * @return {@code true} 表示建议执行全量同步
     */
    public abstract boolean isSyncNeeded();

    /**
     * 执行一轮全量同步：清空进度计数后，按固定顺序扫库并写出 UPDATED 事件。
     * <p>
     * 本方法假设调用方已持有集群锁且已将状态标为 STARTED；方法内不做加锁、
     * 不修改 {@code edqsSyncState}、不启停 API。结束后清空 {@link #counters}
     *（{@link #entityInfoMap} / {@link #keys} 本次同步过程中会占用较多内存，
     * 随对象生命周期回收；若多次 sync，map 会累积，当前实现按单次全量设计）。
     */
    public void sync() {
        log.info("Synchronizing data to EDQS");
        long startTs = System.currentTimeMillis();
        counters.clear();

        syncTenantEntities();
        syncRelations();
        loadKeyDictionary();
        syncAttributes();
        syncLatestTimeseries();

        counters.clear();
        log.info("Finishing synchronizing data to EDQS in {} ms", (System.currentTimeMillis() - startTs));
    }

    /**
     * 进度计数 + 将对象以 {@link EdqsEventType#UPDATED} 交给
     * {@link DefaultEdqsService#processEvent} 异步发往 events 队列。
     *
     * @param tenantId 租户（事件信封必填）
     * @param type     EDQS 对象类型
     * @param object   已构造好的 {@link EdqsObject}
     */
    private void process(TenantId tenantId, ObjectType type, EdqsObject object) {
        AtomicInteger counter = counters.computeIfAbsent(type, t -> new AtomicInteger());
        if (counter.incrementAndGet() % 10000 == 0) {
            log.info("Processed {} {} objects", counter.get(), type);
        }
        edqsService.processEvent(tenantId, type, EdqsEventType.UPDATED, object);
    }

    /**
     * 按 {@link ObjectType#edqsTenantTypes} 逐类型全表游标扫描实体。
     * <p>
     * 使用 Dao {@code findNextBatch(lastId, batchSize)}，从零 UUID 起向后翻页，
     * 直到某批为空。每条写入 {@link #entityInfoMap}，并构造 {@link Entity}
     * 发 UPDATED。耗时按类型分别打日志。
     */
    private void syncTenantEntities() {
        for (ObjectType type : edqsTenantTypes) {
            log.info("Synchronizing {} entities to EDQS", type);
            long ts = System.currentTimeMillis();
            EntityType entityType = type.toEntityType();
            Dao<?> dao = entityDaoRegistry.getDao(entityType);
            UUID lastId = UUID.fromString("00000000-0000-0000-0000-000000000000");
            while (true) {
                var batch = dao.findNextBatch(lastId, entityBatchSize);
                if (batch.isEmpty()) {
                    break;
                }
                for (EntityFields entityFields : batch) {
                    TenantId tenantId = TenantId.fromUUID(entityFields.getTenantId());
                    entityInfoMap.put(entityFields.getId(), new EntityIdInfo(entityType, tenantId));
                    process(tenantId, type, new Entity(entityType, entityFields));
                }
                EntityFields lastRecord = batch.get(batch.size() - 1);
                lastId = lastRecord.getId();
            }
            log.info("Finished synchronizing {} entities to EDQS in {} ms", type, (System.currentTimeMillis() - ts));
        }
    }

    /**
     * 分批同步关系表中的 {@link RelationTypeGroup#COMMON} 关系。
     * <p>
     * 游标键为关系复合主键各字段（from / typeGroup / type / to），
     * 通过 {@link RelationRepository#findNextBatch} 翻页；具体过滤在
     * {@link #processRelationBatch} 中完成。
     */
    private void syncRelations() {
        log.info("Synchronizing relations to EDQS");
        long ts = System.currentTimeMillis();
        UUID lastFromEntityId = UUID.fromString("00000000-0000-0000-0000-000000000000");
        String lastFromEntityType = "";
        String lastRelationTypeGroup = "";
        String lastRelationType = "";
        UUID lastToEntityId = UUID.fromString("00000000-0000-0000-0000-000000000000");
        String lastToEntityType = "";

        while (true) {
            List<RelationEntity> batch = relationRepository.findNextBatch(lastFromEntityId, lastFromEntityType, lastRelationTypeGroup,
                    lastRelationType, lastToEntityId, lastToEntityType, entityBatchSize);
            if (batch.isEmpty()) {
                break;
            }
            processRelationBatch(batch);

            RelationEntity lastRecord = batch.get(batch.size() - 1);
            lastFromEntityId = lastRecord.getFromId();
            lastFromEntityType = lastRecord.getFromType();
            lastRelationTypeGroup = lastRecord.getRelationTypeGroup();
            lastRelationType = lastRecord.getRelationType();
            lastToEntityId = lastRecord.getToId();
            lastToEntityType = lastRecord.getToType();
        }
        log.info("Finished synchronizing relations to EDQS in {} ms", (System.currentTimeMillis() - ts));
    }

    /**
     * 处理一批关系：仅 COMMON 组；{@code fromId} 必须能在 {@link #entityInfoMap} 中解析出租户。
     * <p>
     * from 不存在时打 info（常见于 from 实体类型未纳入 EDQS 或数据不一致）；
     * 单条异常 catch 后继续，避免整批中断。
     *
     * @param relations 当前页关系实体
     */
    private void processRelationBatch(List<RelationEntity> relations) {
        for (RelationEntity relation : relations) {
            try {
                if (RelationTypeGroup.COMMON.name().equals(relation.getRelationTypeGroup())) {
                    EntityIdInfo entityIdInfo = entityInfoMap.get(relation.getFromId());
                    if (entityIdInfo != null) {
                        process(entityIdInfo.tenantId(), RELATION, relation.toData());
                    } else {
                        log.info("Relation from id not found: {} ", relation);
                    }
                }
            } catch (Exception e) {
                log.error("Failed to sync relation: {}", relation, e);
            }
        }
    }

    /**
     * 预加载全部键字典到 {@link #keys}，减少属性/时序同步时逐条查库。
     * <p>
     * 使用 {@link PageDataIterable} 分页拉取；后续 {@link #getStrKeyOrFetchFromDb}
     * 仍可对遗漏 key 回源补齐。
     */
    private void loadKeyDictionary() {
        log.info("Loading key dictionary");
        long ts = System.currentTimeMillis();
        var keyDictionaryEntries = new PageDataIterable<>(keyDictionaryDao::findAll, 10000);
        for (KeyDictionaryEntry keyDictionaryEntry : keyDictionaryEntries) {
            keys.put(keyDictionaryEntry.getKeyId(), keyDictionaryEntry.getKey());
        }
        log.info("Finished loading key dictionary in {} ms", (System.currentTimeMillis() - ts));
    }

    /**
     * 分批同步全部属性 KV。
     * <p>
     * 游标：(entityId, attributeType, attributeKey)；批次大小为 {@link #tsBatchSize}。
     */
    private void syncAttributes() {
        log.info("Synchronizing attributes to EDQS");
        long ts = System.currentTimeMillis();

        UUID lastEntityId = UUID.fromString("00000000-0000-0000-0000-000000000000");
        int lastAttributeType = Integer.MIN_VALUE;
        int lastAttributeKey = Integer.MIN_VALUE;

        while (true) {
            List<AttributeKvEntity> batch = attributesDao.findNextBatch(lastEntityId, lastAttributeType, lastAttributeKey, tsBatchSize);
            if (batch.isEmpty()) {
                break;
            }
            processAttributeBatch(batch);

            AttributeKvEntity lastRecord = batch.get(batch.size() - 1);
            lastEntityId = lastRecord.getId().getEntityId();
            lastAttributeType = lastRecord.getId().getAttributeType();
            lastAttributeKey = lastRecord.getId().getAttributeKey();
        }
        log.info("Finished synchronizing attributes to EDQS in {} ms", (System.currentTimeMillis() - ts));
    }

    /**
     * 将属性批次转为 {@link AttributeKv} 并写出。
     * <ul>
     *   <li>用 {@link #getStrKeyOrFetchFromDb} 还原字符串 key；</li>
     *   <li>用 {@link #entityInfoMap} 还原实体类型与租户，缺失则 debug 跳过；</li>
     *   <li>scope 来自 attributeType 枚举序值。</li>
     * </ul>
     *
     * @param batch 当前页属性实体
     */
    private void processAttributeBatch(List<AttributeKvEntity> batch) {
        for (AttributeKvEntity attribute : batch) {
            try {
                attribute.setStrKey(getStrKeyOrFetchFromDb(attribute.getId().getAttributeKey()));
                UUID entityId = attribute.getId().getEntityId();
                EntityIdInfo entityIdInfo = entityInfoMap.get(entityId);
                if (entityIdInfo == null) {
                    log.debug("Skipping attribute with entity UUID {} as it is not found in entityInfoMap", entityId);
                    continue;
                }
                AttributeKv attributeKv = new AttributeKv(
                        EntityIdFactory.getByTypeAndUuid(entityIdInfo.entityType(), entityId),
                        AttributeScope.valueOf(attribute.getId().getAttributeType()),
                        attribute.toData(),
                        attribute.getVersion());
                process(entityIdInfo.tenantId(), ATTRIBUTE_KV, attributeKv);
            } catch (Exception e) {
                log.error("Failed to sync attribute: {}", attribute, e);
            }
        }
    }

    /**
     * 分批同步最新时序（ts_kv_latest），游标为 (entityId, keyId)。
     */
    private void syncLatestTimeseries() {
        log.info("Synchronizing latest timeseries to EDQS");
        long ts = System.currentTimeMillis();
        UUID lastEntityId = UUID.fromString("00000000-0000-0000-0000-000000000000");
        int lastKey = Integer.MIN_VALUE;

        while (true) {
            List<TsKvLatestEntity> batch = tsKvLatestRepository.findNextBatch(lastEntityId, lastKey, tsBatchSize);
            if (batch.isEmpty()) {
                break;
            }
            processTsKvLatestBatch(batch);

            TsKvLatestEntity lastRecord = batch.get(batch.size() - 1);
            lastEntityId = lastRecord.getEntityId();
            lastKey = lastRecord.getKey();
        }
        log.info("Finished synchronizing latest timeseries to EDQS in {} ms", (System.currentTimeMillis() - ts));
    }

    /**
     * 将最新时序批次转为 {@link LatestTsKv} 并写出。
     * <p>
     * 字典中无字符串 key、或实体不在 {@link #entityInfoMap} 时跳过该行。
     *
     * @param tsKvLatestEntities 当前页最新时序实体
     */
    private void processTsKvLatestBatch(List<TsKvLatestEntity> tsKvLatestEntities) {
        for (TsKvLatestEntity tsKvLatestEntity : tsKvLatestEntities) {
            try {
                String strKey = getStrKeyOrFetchFromDb(tsKvLatestEntity.getKey());
                if (strKey == null) {
                    log.debug("Skipping latest timeseries with key {} as it is not found in key dictionary", tsKvLatestEntity.getKey());
                    continue;
                }
                tsKvLatestEntity.setStrKey(strKey);
                UUID entityUuid = tsKvLatestEntity.getEntityId();
                EntityIdInfo entityIdInfo = entityInfoMap.get(entityUuid);
                if (entityIdInfo != null) {
                    EntityId entityId = EntityIdFactory.getByTypeAndUuid(entityIdInfo.entityType(), entityUuid);
                    LatestTsKv latestTsKv = new LatestTsKv(entityId, tsKvLatestEntity.toData(), tsKvLatestEntity.getVersion());
                    process(entityIdInfo.tenantId(), LATEST_TS_KV, latestTsKv);
                }
            } catch (Exception e) {
                log.error("Failed to sync latest timeseries: {}", tsKvLatestEntity, e);
            }
        }
    }

    /**
     * 按数值 keyId 解析字符串 key：先查 {@link #keys}，未命中再查
     * {@link KeyDictionaryDao#getKey} 并回填缓存；库中也不存在则返回 {@code null}。
     *
     * @param key 键字典 ID
     * @return 字符串 key；无法解析时为 null
     */
    private String getStrKeyOrFetchFromDb(int key) {
        String strKey = keys.get(key);
        if (strKey != null) {
            return strKey;
        } else {
            strKey = keyDictionaryDao.getKey(key);
            if (strKey != null) {
                keys.put(key, strKey);
            }
        }
        return strKey;
    }

    /**
     * 实体 UUID 在同步过程中的附属信息：实体类型 + 所属租户。
     * <p>
     * 供关系 / 属性 / 最新时序在只有 UUID 时补全事件信封与 {@link EntityId}。
     *
     * @param entityType 实体类型
     * @param tenantId   所属租户
     */
    public record EntityIdInfo(EntityType entityType, TenantId tenantId) {
    }

}
