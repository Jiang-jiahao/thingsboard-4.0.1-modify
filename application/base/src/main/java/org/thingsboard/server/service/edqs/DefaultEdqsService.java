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

@Service
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(value = "queue.edqs.sync.enabled", havingValue = "true")
public class DefaultEdqsService implements EdqsService {

    private final EdqsClientQueueFactory queueFactory;
    private final EdqsConverter edqsConverter;
    private final EdqsSyncService edqsSyncService;
    private final EdqsApiService edqsApiService;
    private final DistributedLockService distributedLockService;
    private final AttributesService attributesService;
    private final EdqsPartitionService edqsPartitionService;
    private final TopicService topicService;
    private final TbServiceInfoProvider serviceInfoProvider;
    @Autowired @Lazy
    private TbClusterService clusterService;
    @Autowired @Lazy
    private HashPartitionService hashPartitionService;

    private EdqsProducer eventsProducer;
    private ExecutorService executor;
    private DistributedLock syncLock;

    @PostConstruct
    private void init() {
        executor = ThingsBoardExecutors.newWorkStealingPool(12, getClass());
        eventsProducer = EdqsProducer.builder()
                .producer(queueFactory.createEdqsEventsProducer())
                .partitionService(edqsPartitionService)
                .build();
        syncLock = distributedLockService.getLock("edqs_sync");
    }

    @AfterStartUp(order = AfterStartUp.REGULAR_SERVICE)
    public void onStartUp() {
        if (!serviceInfoProvider.isService(ServiceType.TB_CORE)) {
            return;
        }
        // 只有core类型的服务才需要执行edqs初始化
        executor.submit(() -> {
            try {
                EdqsSyncState syncState = getSyncState();
                //  同步服务认为需要同步 或者 没有找到同步状态记录（首次启动） 或者 上次同步未完成或失败 才需要进行同步
                if (edqsSyncService.isSyncNeeded() || syncState == null || syncState.getStatus() != EdqsSyncStatus.FINISHED) {
                    // 确保只有负责系统分区的核心节点才能发起同步
                    if (hashPartitionService.isSystemPartitionMine(ServiceType.TB_CORE)) {
                        // 发起同步请求
                        processSystemRequest(ToCoreEdqsRequest.builder()
                                .syncRequest(new EdqsSyncRequest())
                                .build());
                    }
                } else if (edqsApiService.isSupported() && edqsApiService.isAutoEnable()) {
                    // 同步已完成且API支持自动启用，则启用API
                    // only if topic/RocksDB is not empty and sync is finished
                    edqsApiService.setEnabled(true);
                }
            } catch (Throwable e) {
                log.error("Failed to start EDQS service", e);
            }
        });
    }

    /**
     * 处理系统同步请求
     * 1. 记录同步请求状态
     * 2. 广播请求到所有核心节点
     *
     * @param request 同步请求
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
     * 处理来自其他core节点的系统消息
     * 主要处理：
     * 1. API启用/禁用指令
     * 2. 同步请求
     *
     * @param msg 系统消息
     */
    @Override
    public void processSystemMsg(ToCoreEdqsMsg msg) {
        executor.submit(() -> {
            log.info("Processing system msg {}", msg);
            try {
                // 处理API启用/禁用指令
                if (msg.getApiEnabled() != null) {
                    edqsApiService.setEnabled(msg.getApiEnabled());
                }

                if (msg.getSyncRequest() != null) {
                    // 获取分布式锁，确保只有一个节点执行同步
                    syncLock.lock();
                    try {
                        // 检查当前同步状态
                        EdqsSyncState syncState = getSyncState();
                        if (syncState != null) {
                            EdqsSyncStatus status = syncState.getStatus();
                            // 如果同步已完成或失败，忽略新请求
                            if (status == EdqsSyncStatus.FINISHED || status == EdqsSyncStatus.FAILED) {
                                log.info("EDQS sync is already " + status + ", ignoring the msg");
                                return;
                            }
                        }
                        // 开始同步，更新状态
                        saveSyncState(EdqsSyncStatus.STARTED);
                        // 执行实际同步逻辑
                        edqsSyncService.sync();
                        // 同步完成，更新状态
                        saveSyncState(EdqsSyncStatus.FINISHED);

                        // 同步完成后处理API状态
                        if (edqsApiService.isSupported())
                            if (edqsApiService.isAutoEnable()) {
                                // 自动启用API，并广播启用指令
                                log.info("EDQS sync is finished, auto-enabling API");
                                broadcast(ToCoreEdqsMsg.builder()
                                        .apiEnabled(Boolean.TRUE)
                                        .build());
                            } else {
                                log.info("EDQS sync is finished, but leaving API disabled");
                            }
                    } catch (Exception e) {
                        log.error("Failed to complete sync", e);
                        // 同步失败，记录失败状态
                        saveSyncState(EdqsSyncStatus.FAILED);
                    } finally {
                        // 释放锁
                        syncLock.unlock();
                    }
                }
            } catch (Throwable e) {
                log.error("Failed to process msg {}", msg, e);
            }
        });
    }

    /**
     * 处理实体更新事件
     * 1. 将实体类型转换为ObjectType
     * 2. 检查是否为EDQS支持的实体类型
     * 3. 调用具体的更新处理方法
     *
     * @param tenantId 租户ID
     * @param entityId 实体ID
     * @param entity 实体对象
     */
    @Override
    public void onUpdate(TenantId tenantId, EntityId entityId, Object entity) {
        EntityType entityType = entityId.getEntityType();
        ObjectType objectType = ObjectType.fromEntityType(entityType);
        // 检查是否为EDQS支持的实体类型
        if (!isEdqsType(tenantId, objectType)) {
            log.trace("[{}][{}] Ignoring update event, type {} not supported", tenantId, entityId, entityType);
            return;
        }
        // 将实体转换为EDQS对象并处理
        onUpdate(tenantId, objectType, edqsConverter.toEntity(entityType, entity));
    }

    /**
     * 处理EDQS对象更新事件
     * 将更新事件发送到EDQS消息队列
     *
     * @param tenantId 租户ID
     * @param objectType 对象类型
     * @param object EDQS对象
     */
    @Override
    public void onUpdate(TenantId tenantId, ObjectType objectType, EdqsObject object) {
        processEvent(tenantId, objectType, EdqsEventType.UPDATED, object);
    }

    /**
     * 处理实体删除事件
     * 1. 将实体类型转换为ObjectType
     * 2. 检查是否为EDQS支持的实体类型
     * 3. 创建删除实体对象并处理
     *
     * @param tenantId 租户ID
     * @param entityId 实体ID
     */
    @Override
    public void onDelete(TenantId tenantId, EntityId entityId) {
        EntityType entityType = entityId.getEntityType();
        ObjectType objectType = ObjectType.fromEntityType(entityType);
        if (!isEdqsType(tenantId, objectType)) {
            log.trace("[{}][{}] Ignoring deletion event, type {} not supported", tenantId, entityId, entityType);
            return;
        }
        // 创建删除实体对象（版本设置为Long.MAX_VALUE表示删除）
        onDelete(tenantId, objectType, new Entity(entityType, entityId.getId(), Long.MAX_VALUE));
    }

    /**
     * 处理EDQS对象删除事件
     * 将删除事件发送到EDQS消息队列
     *
     * @param tenantId 租户ID
     * @param objectType 对象类型
     * @param object EDQS对象
     */
    @Override
    public void onDelete(TenantId tenantId, ObjectType objectType, EdqsObject object) {
        processEvent(tenantId, objectType, EdqsEventType.DELETED, object);
    }

    /**
     * 处理EDQS事件的核心方法
     * 1. 异步构建EDQS事件消息
     * 2. 发送到EDQS消息队列
     * 3. 异常处理确保服务稳定性
     *
     * @param tenantId 租户ID
     * @param objectType 对象类型
     * @param eventType 事件类型（更新/删除）
     * @param object EDQS对象
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
                // 如果对象有版本号，设置版本号
                if (version != null) {
                    eventMsg.setVersion(version);
                }
                // 发送到EDQS消息队列
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
     * 检查是否为EDQS支持的实体类型
     * 根据租户类型（系统租户/普通租户）检查不同的支持类型集合
     *
     * @param tenantId 租户ID
     * @param objectType 对象类型
     * @return true表示支持，false表示不支持
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

    private void broadcast(ToCoreEdqsMsg msg) {
        clusterService.broadcastToCore(ToCoreNotificationMsg.newBuilder()
                .setToEdqsCoreServiceMsg(ToEdqsCoreServiceMsg.newBuilder()
                        .setValue(ByteString.copyFrom(JacksonUtil.writeValueAsBytes(msg))))
                .build());
    }

    /**
     * 获取EDQS同步状态
     * 从服务端属性中读取同步状态信息
     *
     * @return 同步状态对象，如果没有记录则返回null
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
     * 保存EDQS同步状态
     * 将同步状态保存到服务端属性中
     *
     * @param status 要保存的同步状态
     */
    @SneakyThrows
    private void saveSyncState(EdqsSyncStatus status) {
        EdqsSyncState state = new EdqsSyncState(status);
        log.info("New EDQS sync state: {}", state);
        attributesService.save(TenantId.SYS_TENANT_ID, TenantId.SYS_TENANT_ID, AttributeScope.SERVER_SCOPE, new BaseAttributeKvEntry(
                new JsonDataEntry("edqsSyncState", JacksonUtil.toString(state)),
                System.currentTimeMillis())).get(30, TimeUnit.SECONDS);
    }

    @PreDestroy
    private void stop() {
        executor.shutdown();
        eventsProducer.stop();
    }

    /**
     * EDQS同步状态内部类
     * 用于序列化保存同步状态
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class EdqsSyncState {
        private EdqsSyncStatus status;
    }

    /**
     * EDQS同步状态枚举
     * 定义同步过程的各种状态
     */
    private enum EdqsSyncStatus {
        REQUESTED, // 已请求同步
        STARTED, // 同步已开始
        FINISHED, // 同步已完成
        FAILED // 同步失败
    }

}
