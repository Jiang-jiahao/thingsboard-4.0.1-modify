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
package org.thingsboard.server.edqs.repo;

import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.ObjectType;
import org.thingsboard.server.common.data.edqs.AttributeKv;
import org.thingsboard.server.common.data.edqs.DataPoint;
import org.thingsboard.server.common.data.edqs.EdqsEvent;
import org.thingsboard.server.common.data.edqs.EdqsEventType;
import org.thingsboard.server.common.data.edqs.EdqsObject;
import org.thingsboard.server.common.data.edqs.Entity;
import org.thingsboard.server.common.data.edqs.LatestTsKv;
import org.thingsboard.server.common.data.edqs.fields.EntityFields;
import org.thingsboard.server.common.data.edqs.query.QueryResult;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.permission.QueryContext;
import org.thingsboard.server.common.data.query.EntityCountQuery;
import org.thingsboard.server.common.data.query.EntityDataQuery;
import org.thingsboard.server.common.data.query.EntityDataSortOrder;
import org.thingsboard.server.common.data.query.EntityFilter;
import org.thingsboard.server.common.data.query.EntityKeyType;
import org.thingsboard.server.common.data.query.TsValue;
import org.thingsboard.server.common.data.relation.EntityRelation;
import org.thingsboard.server.common.data.relation.RelationTypeGroup;
import org.thingsboard.server.common.stats.EdqsStatsService;
import org.thingsboard.server.edqs.data.ApiUsageStateData;
import org.thingsboard.server.edqs.data.AssetData;
import org.thingsboard.server.edqs.data.CustomerData;
import org.thingsboard.server.edqs.data.DeviceData;
import org.thingsboard.server.edqs.data.EntityData;
import org.thingsboard.server.edqs.data.EntityProfileData;
import org.thingsboard.server.edqs.data.GenericData;
import org.thingsboard.server.edqs.data.RelationsRepo;
import org.thingsboard.server.edqs.data.TenantData;
import org.thingsboard.server.edqs.query.EdqsDataQuery;
import org.thingsboard.server.edqs.query.EdqsQuery;
import org.thingsboard.server.edqs.query.SortableEntityData;
import org.thingsboard.server.edqs.query.processor.EntityQueryProcessor;
import org.thingsboard.server.edqs.query.processor.EntityQueryProcessorFactory;
import org.thingsboard.server.edqs.util.RepositoryUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.thingsboard.server.edqs.util.RepositoryUtils.SORT_ASC;
import static org.thingsboard.server.edqs.util.RepositoryUtils.SORT_DESC;
import static org.thingsboard.server.edqs.util.RepositoryUtils.resolveEntityType;

/**
 * 单个租户的 EDQS 内存仓库：维护该租户下实体、关系、属性、最新时序的内存索引，
 * 并在此之上执行实体计数 / 实体数据查询。
 * <p>
 * 由 {@link DefaultEdqsRepository} 按 {@link TenantId} 懒创建并持有。
 * 写入路径来自 {@link EdqsEvent}（UPDATED / DELETED）；查询路径通过
 * {@link EntityQueryProcessor} 过滤后，在本类内完成排序、分页与结果组装。
 * <p>
 * 内部索引结构：
 * <ul>
 *   <li>{@link #entityMapByType}：按类型 + UUID 点查实体；</li>
 *   <li>{@link #entitySetByType}：按类型维护有序集合（创建时间 + ID 降序），便于遍历；</li>
 *   <li>{@link #relations}：按关系分组维护 {@link RelationsRepo}（COMMON 等）；
 *       DASHBOARD 关系则挂在 {@link CustomerData} 上。</li>
 * </ul>
 * 实体增删改与客户归属变更使用 {@link #entityUpdateLock} 串行化，避免并发写索引不一致。
 */
@Slf4j
public class TenantRepo {

    /** 仅按创建时间比较。 */
    public static final Comparator<EntityData<?>> CREATED_TIME_COMPARATOR = Comparator.comparingLong(ed -> ed.getFields().getCreatedTime());
    /** 创建时间升序，相同时按实体 ID 升序（稳定排序键）。 */
    public static final Comparator<EntityData<?>> CREATED_TIME_AND_ID_COMPARATOR = CREATED_TIME_COMPARATOR
            .thenComparing(EntityData::getId);
    /** 创建时间 + ID 降序；{@link #entitySetByType} 默认使用此比较器。 */
    public static final Comparator<EntityData<?>> CREATED_TIME_AND_ID_DESC_COMPARATOR = CREATED_TIME_AND_ID_COMPARATOR.reversed();

    /**
     * 按实体类型分组的有序实体集合（创建时间 + ID 降序）。
     * <p>
     * 仅包含已写入 {@link EntityFields} 的实体；占位实体（仅有 ID）不会加入。
     */
    private final ConcurrentMap<EntityType, Set<EntityData<?>>> entitySetByType = new ConcurrentHashMap<>();
    /**
     * 按实体类型分组的 ID → 实体数据映射，支持 O(1) 点查；
     * 关系 / 属性到达时可通过 {@link #getOrCreate} 先创建占位对象。
     */
    private final ConcurrentMap<EntityType, ConcurrentMap<UUID, EntityData<?>>> entityMapByType = new ConcurrentHashMap<>();
    /** 关系分组 → 关系仓库；当前主要使用 {@link RelationTypeGroup#COMMON}。 */
    private final ConcurrentMap<RelationTypeGroup, RelationsRepo> relations = new ConcurrentHashMap<>();

    /** 保护实体增删改及客户归属变更的可重入锁。 */
    private final Lock entityUpdateLock = new ReentrantLock();

    private final TenantId tenantId;
    private final EdqsStatsService edqsStatsService;

    public TenantRepo(TenantId tenantId, EdqsStatsService edqsStatsService) {
        this.tenantId = tenantId;
        this.edqsStatsService = edqsStatsService;
    }

    /**
     * 处理一条租户内事件：UPDATED → {@link #addOrUpdate}，DELETED → {@link #remove}。
     *
     * @param event 已反序列化的 EDQS 事件（含实体 / 关系 / 属性 / 最新时序等）
     */
    public void processEvent(EdqsEvent event) {
        EdqsObject edqsObject = event.getObject();
        log.trace("[{}] Processing event: {}", tenantId, event);
        if (event.getEventType() == EdqsEventType.UPDATED) {
            addOrUpdate(edqsObject);
        } else if (event.getEventType() == EdqsEventType.DELETED) {
            remove(edqsObject);
        }
    }

    /**
     * 按对象类型分发新增或更新：关系、属性、最新时序、实体本体。
     */
    public void addOrUpdate(EdqsObject object) {
        if (object instanceof EntityRelation relation) {
            addOrUpdateRelation(relation);
        } else if (object instanceof AttributeKv attributeKv) {
            addOrUpdateAttribute(attributeKv);
        } else if (object instanceof LatestTsKv latestTsKv) {
            addOrUpdateLatestKv(latestTsKv);
        } else if (object instanceof Entity entity) {
            addOrUpdateEntity(entity);
        }
    }

    /**
     * 按对象类型分发删除：关系、属性、最新时序、实体本体。
     */
    public void remove(EdqsObject object) {
        if (object instanceof EntityRelation relation) {
            removeRelation(relation);
        } else if (object instanceof AttributeKv attributeKv) {
            removeAttribute(attributeKv);
        } else if (object instanceof LatestTsKv latestTsKv) {
            removeLatestKv(latestTsKv);
        } else if (object instanceof Entity entity) {
            removeEntity(entity);
        }
    }

    /**
     * 新增或更新关系。
     * <ul>
     *   <li>{@link RelationTypeGroup#COMMON}：写入 {@link RelationsRepo}；</li>
     *   <li>{@link RelationTypeGroup#DASHBOARD}：客户 CONTAINS 仪表盘时，挂到 {@link CustomerData}。</li>
     * </ul>
     */
    private void addOrUpdateRelation(EntityRelation entity) {
        entityUpdateLock.lock();
        try {
            if (RelationTypeGroup.COMMON.equals(entity.getTypeGroup())) {
                RelationsRepo repo = relations.computeIfAbsent(entity.getTypeGroup(), tg -> new RelationsRepo());
                EntityData<?> from = getOrCreate(entity.getFrom());
                EntityData<?> to = getOrCreate(entity.getTo());
                boolean added = repo.add(from, to, entity.getType());
                if (added) {
                    edqsStatsService.reportAdded(ObjectType.RELATION);
                }
            } else if (RelationTypeGroup.DASHBOARD.equals(entity.getTypeGroup())) {
                if (EntityRelation.CONTAINS_TYPE.equals(entity.getType()) && entity.getFrom().getEntityType() == EntityType.CUSTOMER) {
                    CustomerData customerData = (CustomerData) getOrCreate(entity.getFrom());
                    EntityData<?> dashboardData = getOrCreate(entity.getTo());
                    customerData.addOrUpdate(dashboardData);
                }
            }
        } finally {
            entityUpdateLock.unlock();
        }
    }

    /**
     * 删除关系；COMMON 从 {@link RelationsRepo} 移除，DASHBOARD 从客户侧索引移除。
     */
    private void removeRelation(EntityRelation entityRelation) {
        if (RelationTypeGroup.COMMON.equals(entityRelation.getTypeGroup())) {
            RelationsRepo relationsRepo = relations.get(entityRelation.getTypeGroup());
            if (relationsRepo != null) {
                boolean removed = relationsRepo.remove(entityRelation.getFrom().getId(), entityRelation.getTo().getId(), entityRelation.getType());
                if (removed) {
                    edqsStatsService.reportRemoved(ObjectType.RELATION);
                }
            }
        } else if (RelationTypeGroup.DASHBOARD.equals(entityRelation.getTypeGroup())) {
            if (EntityRelation.CONTAINS_TYPE.equals(entityRelation.getType()) && entityRelation.getFrom().getEntityType() == EntityType.CUSTOMER) {
                CustomerData customerData = (CustomerData) get(entityRelation.getFrom());
                if (customerData != null) {
                    customerData.remove(EntityType.DASHBOARD, entityRelation.getTo().getId());
                }
            }
        }
    }

    /**
     * 新增或更新实体本体字段，并维护客户归属索引。
     * <p>
     * 首次写入 fields 时加入 {@link #entitySetByType}；
     * customerId 变化时从旧客户移除并挂到新客户。
     */
    private void addOrUpdateEntity(Entity entity) {
        entityUpdateLock.lock();
        try {
            log.trace("[{}] addOrUpdateEntity: {}", tenantId, entity);
            EntityFields fields = entity.getFields();
            UUID entityId = fields.getId();
            EntityType entityType = entity.getType();

            EntityData entityData = getOrCreate(entityType, entityId);
            EntityFields oldFields = entityData.getFields();
            entityData.setFields(fields);
            if (oldFields == null) {
                getEntitySet(entityType).add(entityData);
            }

            UUID newCustomerId = fields.getCustomerId();
            UUID oldCustomerId = entityData.getCustomerId();
            entityData.setCustomerId(newCustomerId);
            if (entityIdMismatch(oldCustomerId, newCustomerId)) {
                if (oldCustomerId != null) {
                    CustomerData old = (CustomerData) get(EntityType.CUSTOMER, oldCustomerId);
                    if (old != null) {
                        old.remove(entityType, entityId);
                    }
                }
                if (newCustomerId != null) {
                    CustomerData newData = (CustomerData) getOrCreate(EntityType.CUSTOMER, newCustomerId);
                    newData.addOrUpdate(entityData);
                }
            }
        } finally {
            entityUpdateLock.unlock();
        }
    }

    /**
     * 删除实体：从 map / set 移除，上报统计，并从所属客户索引中摘除。
     */
    public void removeEntity(Entity entity) {
        entityUpdateLock.lock();
        try {
            UUID entityId = entity.getFields().getId();
            EntityType entityType = entity.getType();
            EntityData<?> removed = getEntityMap(entityType).remove(entityId);
            if (removed != null) {
                if (removed.getFields() != null) {
                    getEntitySet(entityType).remove(removed);
                }
                edqsStatsService.reportRemoved(entity.type());

                UUID customerId = removed.getCustomerId();
                if (customerId != null) {
                    CustomerData customerData = (CustomerData) get(EntityType.CUSTOMER, customerId);
                    if (customerData != null) {
                        customerData.remove(entityType, entityId);
                    }
                }
            }
        } finally {
            entityUpdateLock.unlock();
        }
    }

    /**
     * 写入或更新实体属性；key 经 {@link KeyDictionary} 转为整型 ID 后存入 {@link EntityData}。
     */
    public void addOrUpdateAttribute(AttributeKv attributeKv) {
        var entityData = getOrCreate(attributeKv.getEntityId());
        if (entityData != null) {
            Integer keyId = KeyDictionary.get(attributeKv.getKey());
            boolean added = entityData.putAttr(keyId, attributeKv.getScope(), attributeKv.getDataPoint());
            if (added) {
                edqsStatsService.reportAdded(ObjectType.ATTRIBUTE_KV);
            }
        }
    }

    /** 删除实体属性。 */
    private void removeAttribute(AttributeKv attributeKv) {
        var entityData = get(attributeKv.getEntityId());
        if (entityData != null) {
            boolean removed = entityData.removeAttr(KeyDictionary.get(attributeKv.getKey()), attributeKv.getScope());
            if (removed) {
                edqsStatsService.reportRemoved(ObjectType.ATTRIBUTE_KV);
            }
        }
    }

    /** 写入或更新实体最新时序值。 */
    public void addOrUpdateLatestKv(LatestTsKv latestTsKv) {
        var entityData = getOrCreate(latestTsKv.getEntityId());
        if (entityData != null) {
            Integer keyId = KeyDictionary.get(latestTsKv.getKey());
            boolean added = entityData.putTs(keyId, latestTsKv.getDataPoint());
            if (added) {
                edqsStatsService.reportAdded(ObjectType.LATEST_TS_KV);
            }
        }
    }

    /** 删除实体最新时序值。 */
    private void removeLatestKv(LatestTsKv latestTsKv) {
        var entityData = get(latestTsKv.getEntityId());
        if (entityData != null) {
            boolean removed = entityData.removeTs(KeyDictionary.get(latestTsKv.getKey()));
            if (removed) {
                edqsStatsService.reportRemoved(ObjectType.LATEST_TS_KV);
            }
        }
    }

    /**
     * 获取指定类型的实体 ID 映射；不存在则懒创建空 map。
     */
    public ConcurrentMap<UUID, EntityData<?>> getEntityMap(EntityType entityType) {
        return entityMapByType.computeIfAbsent(entityType, et -> new ConcurrentHashMap<>());
    }

    //TODO: automatically remove entities that has nothing except the ID.
    /** 按 {@link EntityId} 获取或创建占位 {@link EntityData}。 */
    private EntityData<?> getOrCreate(EntityId entityId) {
        return getOrCreate(entityId.getEntityType(), entityId.getId());
    }

    /**
     * 获取或创建实体数据占位对象。
     * <p>
     * 关系 / 属性可能先于实体本体到达，因此允许仅有 ID 的占位；
     * 新建时上报 {@link EdqsStatsService#reportAdded}。
     */
    private EntityData<?> getOrCreate(EntityType entityType, UUID entityId) {
        return getEntityMap(entityType).computeIfAbsent(entityId, id -> {
            log.debug("[{}] Adding {} {}", tenantId, entityType, id);
            EntityData<?> entityData = constructEntityData(entityType, entityId);
            edqsStatsService.reportAdded(ObjectType.fromEntityType(entityType));
            return entityData;
        });
    }

    private EntityData<?> get(EntityId entityId) {
        return get(entityId.getEntityType(), entityId.getId());
    }

    private EntityData<?> get(EntityType entityType, UUID entityId) {
        return getEntityMap(entityType).get(entityId);
    }

    /**
     * 按实体类型构造具体 {@link EntityData} 子类，并回指本仓库（供查询时取关系等）。
     */
    private EntityData<?> constructEntityData(EntityType entityType, UUID id) {
        EntityData<?> entityData = switch (entityType) {
            case DEVICE -> new DeviceData(id);
            case ASSET -> new AssetData(id);
            case DEVICE_PROFILE, ASSET_PROFILE -> new EntityProfileData(id, entityType);
            case CUSTOMER -> new CustomerData(id);
            case TENANT -> new TenantData(id);
            case API_USAGE_STATE -> new ApiUsageStateData(id);
            default -> new GenericData(entityType, id);
        };
        entityData.setRepo(this);
        return entityData;
    }

    /** 判断新旧 customerId 是否发生变化（含 null ↔ 非 null）。 */
    private static boolean entityIdMismatch(UUID oldOrNull, UUID newOrNull) {
        if (oldOrNull == null) {
            return newOrNull != null;
        } else {
            return !oldOrNull.equals(newOrNull);
        }
    }

    /**
     * 获取指定类型的有序实体集合；不存在则懒创建
     * （比较器为 {@link #CREATED_TIME_AND_ID_DESC_COMPARATOR}）。
     */
    public Set<EntityData<?>> getEntitySet(EntityType entityType) {
        return entitySetByType.computeIfAbsent(entityType, et -> new ConcurrentSkipListSet<>(CREATED_TIME_AND_ID_DESC_COMPARATOR));
    }

    /**
     * 实体数据查询：转换查询 → 构建权限上下文 → 过滤 → 排序分页 → 组装 {@link QueryResult}。
     *
     * @param customerId            客户范围（可为 null）
     * @param oldQuery              对外 API 形态的实体数据查询
     * @param ignorePermissionCheck 是否跳过权限校验
     * @return 分页结果
     */
    public PageData<QueryResult> findEntityDataByQuery(CustomerId customerId, EntityDataQuery oldQuery, boolean ignorePermissionCheck) {
        EdqsDataQuery query = RepositoryUtils.toNewQuery(oldQuery);
        QueryContext ctx = buildContext(customerId, query.getEntityFilter(), ignorePermissionCheck);
        EntityQueryProcessor queryProcessor = EntityQueryProcessorFactory.create(this, ctx, query);
        return sortAndConvert(query, queryProcessor.processQuery(), ctx);
    }

    /**
     * 实体计数查询：转换查询 → 构建上下文 → 由 {@link EntityQueryProcessor#count()} 返回匹配数。
     */
    public long countEntitiesByQuery(CustomerId customerId, EntityCountQuery oldQuery, boolean ignorePermissionCheck) {
        EdqsQuery query = RepositoryUtils.toNewQuery(oldQuery);
        QueryContext ctx = buildContext(customerId, query.getEntityFilter(), ignorePermissionCheck);
        EntityQueryProcessor queryProcessor = EntityQueryProcessorFactory.create(this, ctx, query);
        return queryProcessor.count();
    }

    /**
     * 对过滤结果做 Top-N 排序与分页，再转为 {@link QueryResult} 列表。
     * <p>
     * 当前实现用 {@link TreeSet} 维护前 {@code offset + pageSize} 条，避免对全量结果做完整排序；
     * 注释中保留了 PriorityQueue / TimSort 等备选实现。
     */
    private PageData<QueryResult> sortAndConvert(EdqsDataQuery query, List<SortableEntityData> data, QueryContext ctx) {
        int totalSize = data.size();
        int totalPages = (int) Math.ceil((float) totalSize / query.getPageSize());
        int offset = query.getPage() * query.getPageSize();
        if (offset > totalSize) {
            return new PageData<>(Collections.emptyList(), totalPages, totalSize, false);
        } else {
            Comparator<SortableEntityData> comparator = EntityDataSortOrder.Direction.ASC.equals(query.getSortDirection()) ? SORT_ASC : SORT_DESC;
            long startTs = System.nanoTime();
//          IMPLEMENTATION THAT IS BASED ON PRIORITY_QUEUE
//            var requiredSize = Math.min(offset + query.getPageSize(), totalSize);
//            PriorityQueue<SortableEntityData> topN = new PriorityQueue<>(requiredSize, comparator.reversed());
//            for (SortableEntityData item : data) {
//                topN.add(item);
//                if (topN.size() > requiredSize) {
//                    topN.poll();
//                }
//            }
//            List<SortableEntityData> result = new ArrayList<>(topN);
//            Collections.reverse(result);
//            result = result.subList(offset, requiredSize);
//          IMPLEMENTATION THAT IS BASED ON TREE SET  (For offset + query.getPageSize() << totalSize)
            var requiredSize = Math.min(offset + query.getPageSize(), totalSize);
            TreeSet<SortableEntityData> topNSet = new TreeSet<>(comparator);
            for (SortableEntityData sp : data) {
                topNSet.add(sp);
                if (topNSet.size() > requiredSize) {
                    topNSet.pollLast();
                }
            }
            var result = topNSet.stream().skip(offset).limit(query.getPageSize()).collect(Collectors.toList());
//          IMPLEMENTATION THAT IS BASED ON TIM SORT (For offset + query.getPageSize() > totalSize / 2)
//            data.sort(comparator);
//            var result = data.subList(offset, endIndex);
            log.trace("EDQ Sorted in {}", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTs));
            return new PageData<>(toQueryResult(result, query, ctx), totalPages, totalSize, totalSize > requiredSize);
        }
    }

    /**
     * 将排序后的实体数据转为 API 返回结构：按请求字段组装 ENTITY_FIELD / 最新值等 {@link TsValue}。
     */
    private List<QueryResult> toQueryResult(List<SortableEntityData> data, EdqsDataQuery query, QueryContext ctx) {
        long ts = System.currentTimeMillis();
        List<QueryResult> results = new ArrayList<>(data.size());
        for (SortableEntityData entityData : data) {
            Map<EntityKeyType, Map<String, TsValue>> latest = new HashMap<>();
            for (var key : query.getEntityFields()) {
                DataPoint dp = entityData.getEntityData().getDataPoint(key, ctx);
                TsValue v = RepositoryUtils.toTsValue(ts, dp);
                latest.computeIfAbsent(EntityKeyType.ENTITY_FIELD, t -> new HashMap<>()).put(key.key(), v);
            }
            for (var key : query.getLatestValues()) {
                DataPoint dp = entityData.getEntityData().getDataPoint(key, ctx);
                TsValue v = RepositoryUtils.toTsValue(ts, dp);
                latest.computeIfAbsent(key.type(), t -> new HashMap<>()).put(KeyDictionary.get(key.keyId()), v);
            }

            results.add(new QueryResult(entityData.getEntityId(), latest));
        }
        return results;
    }

    /** 构建查询上下文（租户、客户、过滤解析出的实体类型、是否忽略权限）。 */
    private QueryContext buildContext(CustomerId customerId, EntityFilter filter, boolean ignorePermissionCheck) {
        return new QueryContext(tenantId, customerId, resolveEntityType(filter), ignorePermissionCheck);
    }

    public TenantId getTenantId() {
        return tenantId;
    }

    /**
     * 获取指定关系分组的仓库；不存在则懒创建。
     */
    public RelationsRepo getRelations(RelationTypeGroup relationTypeGroup) {
        return relations.computeIfAbsent(relationTypeGroup, type -> new RelationsRepo());
    }

    /**
     * 解析所有者（租户 / 客户）显示名；其它实体类型不支持。
     *
     * @throws RuntimeException 实体类型非 CUSTOMER / TENANT 时
     */
    public String getOwnerEntityName(EntityId entityId) {
        EntityType entityType = entityId.getEntityType();
        return switch (entityType) {
            case CUSTOMER, TENANT -> {
                EntityFields fields = get(entityId).getFields();
                yield fields != null ? fields.getName() : "";
            }
            default -> throw new RuntimeException("Unsupported entity type: " + entityType);
        };
    }

}
