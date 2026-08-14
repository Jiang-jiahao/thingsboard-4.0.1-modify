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

import com.google.protobuf.ByteString;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.common.util.ThingsBoardExecutors;
import org.thingsboard.server.cluster.TbClusterService;
import org.thingsboard.server.common.data.AttributeScope;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.ObjectType;
import org.thingsboard.server.common.data.edqs.EdqsEventType;
import org.thingsboard.server.common.data.edqs.EdqsObject;
import org.thingsboard.server.common.data.edqs.EdqsSyncRequest;
import org.thingsboard.server.common.data.edqs.Entity;
import org.thingsboard.server.common.data.edqs.ToCoreEdqsMsg;
import org.thingsboard.server.common.data.edqs.ToCoreEdqsRequest;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.BaseAttributeKvEntry;
import org.thingsboard.server.common.data.kv.JsonDataEntry;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.msg.edqs.EdqsApiService;
import org.thingsboard.server.common.msg.edqs.EdqsService;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.dao.attributes.AttributesService;
import org.thingsboard.server.edqs.processor.EdqsProducer;
import org.thingsboard.server.edqs.state.EdqsPartitionService;
import org.thingsboard.server.edqs.util.EdqsConverter;
import org.thingsboard.server.gen.transport.TransportProtos.EdqsEventMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToCoreNotificationMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToEdqsCoreServiceMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToEdqsMsg;
import org.thingsboard.server.queue.discovery.HashPartitionService;
import org.thingsboard.server.queue.discovery.TbServiceInfoProvider;
import org.thingsboard.server.queue.discovery.TopicService;
import org.thingsboard.server.queue.environment.DistributedLock;
import org.thingsboard.server.queue.environment.DistributedLockService;
import org.thingsboard.server.queue.provider.EdqsClientQueueFactory;
import org.thingsboard.server.queue.util.AfterStartUp;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Core / Monolith 侧 {@link EdqsService} 默认实现：EDQS 的<strong>写路径与集群协作中枢</strong>。
 * <p>
 * 本类<strong>不</strong>维护内存查询索引（那是 {@code EdqsProcessor} / {@code TenantRepo} 的职责），
 * 也不直接执行实体查询（查询走 {@link EdqsApiService}）。它负责把业务侧的实体变更
 * 变成可投递的 EDQS 事件，并协调「全量同步 → 打开查询 API」的集群流程。
 * <p>
 * <b>生效条件：</b>{@code queue.edqs.sync.enabled=true}。关闭时由 {@code DummyEdqsService} 占位。
 * <p>
 * <b>职责概览：</b>
 * <ol>
 *   <li><b>增量写：</b>DAO / {@link EdqsListener} 等调用 {@link #onUpdate}/{@link #onDelete}，
 *       经类型过滤后序列化为 {@link ToEdqsMsg}，由 {@link EdqsProducer} 发到 events Topic
 *       （Kafka）或内存队列（in-memory）；</li>
 *   <li><b>全量同步：</b>启动时若 topic 为空或持久化状态未 FINISHED，由持有系统分区的 Core
 *       发起 {@link EdqsSyncRequest}；集群内抢 {@code edqs_sync} 锁后，由
 *       {@link EdqsSyncService} 从 DB 批量灌事件；</li>
 *   <li><b>API 启停：</b>同步成功且 {@code api.auto_enable=true} 时，向所有 Core 广播
 *       {@code apiEnabled=true}，使 {@link EdqsApiService#isEnabled()} 变为 true，
 *       此后实体查询才转发到 EDQS；否则查询继续走 PostgreSQL。</li>
 * </ol>
 * <p>
 * <b>与读路径的分工：</b>
 * <ul>
 *   <li>本类 + {@link EdqsClientQueueFactory#createEdqsEventsProducer()}：写 events；</li>
 *   <li>{@link EdqsApiService} + {@code createEdqsRequestTemplate()}：发查询请求；</li>
 *   <li>独立 EDQS 或同进程 local 模式下的 {@code EdqsProcessor}：消费事件、答查询。</li>
 * </ul>
 * <p>
 * <b>同步状态持久化：</b>写在系统租户（{@link TenantId#SYS_TENANT_ID}）的
 * SERVER_SCOPE 属性 {@code edqsSyncState} 中，集群各节点可见，用于避免重复全量同步。
 *
 * @see EdqsService
 * @see EdqsApiService
 * @see EdqsSyncService
 * @see EdqsListener
 */
@Service
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(value = "queue.edqs.sync.enabled", havingValue = "true")
public class DefaultEdqsService implements EdqsService {

    /** 创建 events 生产者（Client 侧队列工厂，不创建 EDQS 服务端消费者）。 */
    private final EdqsClientQueueFactory queueFactory;
    /** 将领域对象 / {@link EdqsObject} 序列化为事件 payload。 */
    private final EdqsConverter edqsConverter;
    /**
     * 全量同步实现：Kafka 模式下按 topic 是否为空判断是否需要 sync；
     * in-memory 模式由 {@link LocalEdqsSyncService} 承担。
     */
    private final EdqsSyncService edqsSyncService;
    /**
     * 查询 API 客户端；本类只负责在同步完成后（可选）{@link EdqsApiService#setEnabled}，
     * 不发起具体查询。
     */
    private final EdqsApiService edqsApiService;
    /** 用于获取集群级 {@link #syncLock}，保证全量同步单飞。 */
    private final DistributedLockService distributedLockService;
    /** 读写 {@code edqsSyncState} 系统属性。 */
    private final AttributesService attributesService;
    /** 解析事件应发往的 EDQS 分区（按租户等策略）。 */
    private final EdqsPartitionService edqsPartitionService;
    private final TopicService topicService;
    /** 判断当前进程是否具备 TB_CORE 角色（仅 Core 参与启动同步编排）。 */
    private final TbServiceInfoProvider serviceInfoProvider;
    /** 向所有 Core 广播系统通知；延迟注入避免循环依赖。 */
    @Autowired @Lazy
    private TbClusterService clusterService;
    /** 判断本节点是否持有 Core 系统分区（谁有资格发起首次 sync）。 */
    @Autowired @Lazy
    private HashPartitionService hashPartitionService;

    /**
     * 封装 {@link EdqsClientQueueFactory#createEdqsEventsProducer()} 的发送器，
     * 按租户/对象类型选择分区后写入 events Topic。
     */
    private EdqsProducer eventsProducer;
    /**
     * 异步线程池：增量事件发送、系统消息处理、启动时 sync 判定均在此执行，
     * 避免阻塞 DAO 事务提交后的调用线程或 Spring 启动线程。
     */
    private ExecutorService executor;
    /**
     * 分布式锁名 {@code edqs_sync}：多个 Core 收到同一广播时，仅一个节点执行
     * {@link EdqsSyncService#sync()}。
     */
    private DistributedLock syncLock;

    /**
     * 初始化写路径资源：工作窃取线程池、事件生产者、同步锁。
     * <p>
     * 此时尚未触发全量同步；同步判定在 {@link #onStartUp()} 中进行。
     */
    @PostConstruct
    private void init() {
        executor = ThingsBoardExecutors.newWorkStealingPool(12, getClass());
        eventsProducer = EdqsProducer.builder()
                .producer(queueFactory.createEdqsEventsProducer())
                .partitionService(edqsPartitionService)
                .build();
        syncLock = distributedLockService.getLock("edqs_sync");
    }

    /**
     * 应用就绪后由 Core 节点执行的 EDQS 启动编排。
     * <p>
     * 非 {@link ServiceType#TB_CORE} 进程直接返回（例如纯 Rule Engine 不会跑此逻辑）。
     * <p>
     * 分支逻辑：
     * <ol>
     *   <li>若 {@link EdqsSyncService#isSyncNeeded()} 为 true，或属性中无状态，
     *       或状态不是 {@link EdqsSyncStatus#FINISHED}：仅当本节点持有 Core
     *       <b>系统分区</b>时，调用 {@link #processSystemRequest} 发起全量同步
     *       （先落 REQUESTED，再广播，最终由抢到锁的节点真正 sync）；</li>
     *   <li>否则认为历史同步已完成：若 API 支持且 {@code auto_enable=true}，
     *       直接在本节点 {@link EdqsApiService#setEnabled(true)}，无需再灌数。</li>
     * </ol>
     * 整体提交到 {@link #executor}，不阻塞 AfterStartUp 其它服务。
     */
    @AfterStartUp(order = AfterStartUp.REGULAR_SERVICE)
    public void onStartUp() {
        if (!serviceInfoProvider.isService(ServiceType.TB_CORE)) {
            return;
        }
        executor.submit(() -> {
            try {
                EdqsSyncState syncState = getSyncState();
                if (edqsSyncService.isSyncNeeded() || syncState == null || syncState.getStatus() != EdqsSyncStatus.FINISHED) {
                    if (hashPartitionService.isSystemPartitionMine(ServiceType.TB_CORE)) {
                        processSystemRequest(ToCoreEdqsRequest.builder()
                                .syncRequest(new EdqsSyncRequest())
                                .build());
                    }
                } else if (edqsApiService.isSupported() && edqsApiService.isAutoEnable()) {
                    edqsApiService.setEnabled(true);
                }
            } catch (Throwable e) {
                log.error("Failed to start EDQS service", e);
            }
        });
    }

    /**
     * 处理「系统级」EDQS 请求（通常来自本节点启动逻辑或管理入口）。
     * <p>
     * 若请求携带 {@link EdqsSyncRequest}：先把全局状态写成
     * {@link EdqsSyncStatus#REQUESTED}（让其它节点看到「同步已发起」），
     * 再 {@link #broadcast} 成 {@link ToCoreEdqsMsg}，由各 Core 的
     * {@link #processSystemMsg} 接收。真正执行 sync 的是抢到锁且状态尚未
     * FINISHED/FAILED 的那一个节点。
     *
     * @param request 系统请求；当前主要使用其中的 syncRequest 字段
     */
    @Override
    public void processSystemRequest(ToCoreEdqsRequest request) {
        log.info("Processing system request {}", request);
        if (request.getSyncRequest() != null) {
            saveSyncState(EdqsSyncStatus.REQUESTED);
        }
        broadcast(request.toInternalMsg());
    }

    /**
     * 处理集群广播到本 Core 的 EDQS 系统消息（异步）。
     * <p>
     * 支持两类互不排斥的指令：
     * <ul>
     *   <li><b>API 启停：</b>{@code apiEnabled != null} 时直接调用
     *       {@link EdqsApiService#setEnabled}，使本节点查询是否走 EDQS；</li>
     *   <li><b>全量同步：</b>{@code syncRequest != null} 时抢 {@link #syncLock}。
     *       若状态已是 FINISHED 或 FAILED 则忽略（防止重复灌数或失败后被再次触发时
     *       误跑——FAILED 也忽略，需人工/重启等重新进入 REQUESTED 流程）。
     *       否则状态 → STARTED → {@link EdqsSyncService#sync()} → FINISHED；
     *       成功且支持 API 时，若 {@code auto_enable} 则再广播 {@code apiEnabled=true}
     *       让<strong>所有</strong> Core 打开查询开关；异常则标记 FAILED。</li>
     * </ul>
     *
     * @param msg 集群内广播的系统消息
     */
    @Override
    public void processSystemMsg(ToCoreEdqsMsg msg) {
        executor.submit(() -> {
            log.info("Processing system msg {}", msg);
            try {
                if (msg.getApiEnabled() != null) {
                    edqsApiService.setEnabled(msg.getApiEnabled());
                }

                if (msg.getSyncRequest() != null) {
                    syncLock.lock();
                    try {
                        EdqsSyncState syncState = getSyncState();
                        if (syncState != null) {
                            EdqsSyncStatus status = syncState.getStatus();
                            if (status == EdqsSyncStatus.FINISHED || status == EdqsSyncStatus.FAILED) {
                                log.info("EDQS sync is already " + status + ", ignoring the msg");
                                return;
                            }
                        }
                        saveSyncState(EdqsSyncStatus.STARTED);
                        edqsSyncService.sync();
                        saveSyncState(EdqsSyncStatus.FINISHED);

                        if (edqsApiService.isSupported())
                            if (edqsApiService.isAutoEnable()) {
                                log.info("EDQS sync is finished, auto-enabling API");
                                broadcast(ToCoreEdqsMsg.builder()
                                        .apiEnabled(Boolean.TRUE)
                                        .build());
                            } else {
                                log.info("EDQS sync is finished, but leaving API disabled");
                            }
                    } catch (Exception e) {
                        log.error("Failed to complete sync", e);
                        saveSyncState(EdqsSyncStatus.FAILED);
                    } finally {
                        syncLock.unlock();
                    }
                }
            } catch (Throwable e) {
                log.error("Failed to process msg {}", msg, e);
            }
        });
    }

    /**
     * 实体更新入口（按 {@link EntityId} + 领域对象）。
     * <p>
     * 先将实体类型映射为 {@link ObjectType}，经 {@link #isEdqsType} 过滤后，
     * 用 {@link EdqsConverter#toEntity} 转成 EDQS {@link Entity}，再投递 UPDATED。
     * 典型调用链：事务提交 → {@link EdqsListener} → 本方法。
     *
     * @param tenantId 租户
     * @param entityId 实体 ID（含类型）
     * @param entity   DAO/业务层实体对象（Device、Asset 等）
     */
    @Override
    public void onUpdate(TenantId tenantId, EntityId entityId, Object entity) {
        EntityType entityType = entityId.getEntityType();
        ObjectType objectType = ObjectType.fromEntityType(entityType);
        if (!isEdqsType(tenantId, objectType)) {
            log.trace("[{}][{}] Ignoring update event, type {} not supported", tenantId, entityId, entityType);
            return;
        }
        onUpdate(tenantId, objectType, edqsConverter.toEntity(entityType, entity));
    }

    /**
     * 已构造好的 {@link EdqsObject} 更新入口（属性、关系、最新时序、同步灌数等直接走此重载）。
     *
     * @param tenantId   租户
     * @param objectType EDQS 对象类型
     * @param object     待索引对象（含 key / version）
     */
    @Override
    public void onUpdate(TenantId tenantId, ObjectType objectType, EdqsObject object) {
        processEvent(tenantId, objectType, EdqsEventType.UPDATED, object);
    }

    /**
     * 实体删除入口。
     * <p>
     * 删除时只知道 ID，构造最小 {@link Entity}（id + {@code version = Long.MAX_VALUE}），
     * 使下游版本比较时删除事件不易被旧更新覆盖。再投递 DELETED。
     *
     * @param tenantId 租户
     * @param entityId 被删实体 ID
     */
    @Override
    public void onDelete(TenantId tenantId, EntityId entityId) {
        EntityType entityType = entityId.getEntityType();
        ObjectType objectType = ObjectType.fromEntityType(entityType);
        if (!isEdqsType(tenantId, objectType)) {
            log.trace("[{}][{}] Ignoring deletion event, type {} not supported", tenantId, entityId, entityType);
            return;
        }
        // 版本设为 Long.MAX_VALUE，表示删除语义
        onDelete(tenantId, objectType, new Entity(entityType, entityId.getId(), Long.MAX_VALUE));
    }

    /**
     * 已构造好的 {@link EdqsObject} 删除入口。
     *
     * @param tenantId   租户
     * @param objectType 对象类型
     * @param object     删除语义对象（属性/关系删除时通常带 key，实体删除见上一方法）
     */
    @Override
    public void onDelete(TenantId tenantId, ObjectType objectType, EdqsObject object) {
        processEvent(tenantId, objectType, EdqsEventType.DELETED, object);
    }

    /**
     * 将单条变更异步封装为 {@link ToEdqsMsg} 并发送到 EDQS events 队列。
     * <p>
     * 消息字段：
     * <ul>
     *   <li>{@code key} / {@code version}：来自 {@link EdqsObject#key()} / {@link EdqsObject#version()}，
     *       供 EDQS 侧去重与 RocksDB/state 键使用；</li>
     *   <li>{@code data}：{@link EdqsConverter#serialize} 后的字节（实体多为 JSON，
     *       属性/最新时序多为 Protobuf）；</li>
     *   <li>{@code eventType}：UPDATED 或 DELETED；</li>
     *   <li>外层带 tenantId、发送时间戳。</li>
     * </ul>
     * 发送失败只打 error 日志，不向调用方抛出，以免拖垮业务写路径。
     *
     * @param tenantId   租户
     * @param objectType 对象类型（写入 eventMsg.objectType）
     * @param eventType  事件类型
     * @param object     业务对象
     */
    protected void processEvent(TenantId tenantId, ObjectType objectType, EdqsEventType eventType, EdqsObject object) {
        executor.submit(() -> {
            try {
                String key = object.key();
                Long version = object.version();
                EdqsEventMsg.Builder eventMsg = EdqsEventMsg.newBuilder()
                        .setKey(key)
                        .setObjectType(objectType.name())
                        .setData(ByteString.copyFrom(edqsConverter.serialize(objectType, object)))
                        .setEventType(eventType.name());
                if (version != null) {
                    eventMsg.setVersion(version);
                }
                eventsProducer.send(tenantId, objectType, key, ToEdqsMsg.newBuilder()
                        .setTenantIdMSB(tenantId.getId().getMostSignificantBits())
                        .setTenantIdLSB(tenantId.getId().getLeastSignificantBits())
                        .setTs(System.currentTimeMillis())
                        .setEventMsg(eventMsg)
                        .build());
            } catch (Throwable e) {
                log.error("[{}] Failed to push {} event for {} {}", tenantId, eventType, objectType, object, e);
            }
        });
    }

    /**
     * 判断该对象类型是否应由 EDQS 索引。
     * <ul>
     *   <li>普通租户：{@link ObjectType#edqsTypes}（设备、资产、关系、属性、最新时序等）；</li>
     *   <li>系统租户：{@link ObjectType#edqsSystemTypes}（仅系统侧需要进 EDQS 的类型）。</li>
     * </ul>
     *
     * @param tenantId   租户（用于区分系统租户）
     * @param objectType 对象类型；null 视为不支持
     * @return true 表示需要发事件
     */
    private boolean isEdqsType(TenantId tenantId, ObjectType objectType) {
        if (objectType == null) {
            return false;
        }
        if (!tenantId.isSysTenantId()) {
            return ObjectType.edqsTypes.contains(objectType);
        } else {
            return ObjectType.edqsSystemTypes.contains(objectType);
        }
    }

    /**
     * 将 {@link ToCoreEdqsMsg} 序列化后，通过 Core 通知通道广播到集群所有 Core 节点。
     * <p>
     * 用于：发起同步、同步完成后打开 API 等需要「每个 Core 本地状态一致」的场景。
     *
     * @param msg 系统消息（syncRequest / apiEnabled 等）
     */
    private void broadcast(ToCoreEdqsMsg msg) {
        clusterService.broadcastToCore(ToCoreNotificationMsg.newBuilder()
                .setToEdqsCoreServiceMsg(ToEdqsCoreServiceMsg.newBuilder()
                        .setValue(ByteString.copyFrom(JacksonUtil.writeValueAsBytes(msg))))
                .build());
    }

    /**
     * 从系统租户 SERVER_SCOPE 属性 {@code edqsSyncState} 读取同步状态 JSON。
     * <p>
     * 阻塞等待最多 30 秒。无属性记录时返回 {@code null}（视为从未成功同步完）。
     *
     * @return 当前同步状态；不存在则为 null
     */
    @SneakyThrows
    private EdqsSyncState getSyncState() {
        EdqsSyncState state = attributesService.find(TenantId.SYS_TENANT_ID, TenantId.SYS_TENANT_ID, AttributeScope.SERVER_SCOPE, "edqsSyncState").get(30, TimeUnit.SECONDS)
                .flatMap(KvEntry::getJsonValue)
                .map(value -> JacksonUtil.fromString(value, EdqsSyncState.class))
                .orElse(null);
        log.info("EDQS sync state: {}", state);
        return state;
    }

    /**
     * 将同步状态以 JSON 属性写回系统租户，供集群其它节点与下次启动读取。
     *
     * @param status 新状态（REQUESTED / STARTED / FINISHED / FAILED）
     */
    @SneakyThrows
    private void saveSyncState(EdqsSyncStatus status) {
        EdqsSyncState state = new EdqsSyncState(status);
        log.info("New EDQS sync state: {}", state);
        attributesService.save(TenantId.SYS_TENANT_ID, TenantId.SYS_TENANT_ID, AttributeScope.SERVER_SCOPE, new BaseAttributeKvEntry(
                new JsonDataEntry("edqsSyncState", JacksonUtil.toString(state)),
                System.currentTimeMillis())).get(30, TimeUnit.SECONDS);
    }

    /**
     * 释放写路径资源：关闭异步线程池并停止事件生产者。
     */
    @PreDestroy
    private void stop() {
        executor.shutdown();
        eventsProducer.stop();
    }

    /**
     * 持久化在系统属性中的同步状态载体（JSON 反序列化用）。
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class EdqsSyncState {
        /** 当前所处阶段，见 {@link EdqsSyncStatus}。 */
        private EdqsSyncStatus status;
    }

    /**
     * 全量同步生命周期。
     * <ul>
     *   <li>{@link #REQUESTED}：已发起，等待某 Core 抢锁执行；</li>
     *   <li>{@link #STARTED}：正在从 DB 灌事件；</li>
     *   <li>{@link #FINISHED}：成功；启动时若为此状态且 auto_enable，可直接开 API；</li>
     *   <li>{@link #FAILED}：失败；后续广播带 sync 的消息会被忽略，避免自动重试刷库。</li>
     * </ul>
     */
    private enum EdqsSyncStatus {
        REQUESTED, // 已请求
        STARTED,   // 执行中
        FINISHED,  // 成功完成
        FAILED     // 失败
    }

}
