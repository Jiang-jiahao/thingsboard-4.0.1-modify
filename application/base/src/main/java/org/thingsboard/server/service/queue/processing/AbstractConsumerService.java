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
package org.thingsboard.server.service.queue.processing;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.thingsboard.common.util.ThingsBoardExecutors;
import org.thingsboard.common.util.ThingsBoardThreadFactory;
import org.thingsboard.server.actors.ActorSystemContext;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.AssetProfileId;
import org.thingsboard.server.common.data.id.CalculatedFieldId;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.DeviceProfileId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.id.TenantProfileId;
import org.thingsboard.server.common.data.plugin.ComponentLifecycleEvent;
import org.thingsboard.server.common.msg.plugin.ComponentLifecycleMsg;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.common.msg.queue.TbCallback;
import org.thingsboard.server.dao.tenant.TbTenantProfileCache;
import org.thingsboard.server.queue.TbQueueConsumer;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;
import org.thingsboard.server.queue.common.consumer.QueueConsumerManager;
import org.thingsboard.server.queue.discovery.PartitionService;
import org.thingsboard.server.queue.discovery.TbApplicationEventListener;
import org.thingsboard.server.queue.discovery.event.PartitionChangeEvent;
import org.thingsboard.server.queue.util.AfterStartUp;
import org.thingsboard.server.service.apiusage.TbApiUsageStateService;
import org.thingsboard.server.service.cf.CalculatedFieldCache;
import org.thingsboard.server.service.profile.TbAssetProfileCache;
import org.thingsboard.server.service.profile.TbDeviceProfileCache;
import org.thingsboard.server.service.queue.TbPackCallback;
import org.thingsboard.server.service.queue.TbPackProcessingContext;
import org.thingsboard.server.service.security.auth.jwt.settings.JwtSettingsService;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 消费者服务的基类
 * @param <N>
 */
@RequiredArgsConstructor
public abstract class AbstractConsumerService<N extends com.google.protobuf.GeneratedMessageV3> extends TbApplicationEventListener<PartitionChangeEvent> {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    // Actor系统上下文
    protected final ActorSystemContext actorContext;

    // 租户配置缓存
    protected final TbTenantProfileCache tenantProfileCache;

    // 设备配置缓存
    protected final TbDeviceProfileCache deviceProfileCache;

    // 资产配置缓存
    protected final TbAssetProfileCache assetProfileCache;

    // 计算字段缓存
    protected final CalculatedFieldCache calculatedFieldCache;

    // API使用状态服务
    protected final TbApiUsageStateService apiUsageStateService;

    // 分区管理服务
    protected final PartitionService partitionService;

    // 应用事件发布器
    protected final ApplicationEventPublisher eventPublisher;

    // JWT配置服务
    protected final JwtSettingsService jwtSettingsService;

    // 通知消息消费者管理器
    protected QueueConsumerManager<TbProtoQueueMsg<N>> nfConsumer;

    // 消费者线程池（动态大小）
    protected ExecutorService consumersExecutor;

    // 管理线程池（固定大小）
    protected ExecutorService mgmtExecutor;

    // 调度线程池（单线程）
    protected ScheduledExecutorService scheduler;

    /**
     * 服务初始化（需在子类构造后调用）
     * @param prefix 线程名前缀（用于标识服务类型）
     */
    public void init(String prefix) {
        // 创建线程池（命名规范：<prefix>-<role>）
        this.consumersExecutor = Executors.newCachedThreadPool(ThingsBoardThreadFactory.forName(prefix + "-consumer"));
        this.mgmtExecutor = ThingsBoardExecutors.newWorkStealingPool(getMgmtThreadPoolSize(), prefix + "-mgmt");
        this.scheduler = ThingsBoardExecutors.newSingleThreadScheduledExecutor(prefix + "-consumer-scheduler");

        // 根据子类服务类型来构建通知消费者管理器
        this.nfConsumer = QueueConsumerManager.<TbProtoQueueMsg<N>>builder()
                .name(getServiceType().getLabel() + " Notifications")
                .msgPackProcessor(this::processNotifications) // 绑定消息处理函数
                .pollInterval(getNotificationPollDuration()) // 配置轮询间隔
                .consumerCreator(this::createNotificationsConsumer) // 绑定消费者创建器
                .consumerExecutor(consumersExecutor) // 指定执行线程池
                .threadPrefix("notifications") // 内部线程前缀
                .build();
    }

    /**
     * 系统启动后初始化消费者（通过{@link AfterStartUp}注解控制执行顺序）
     */
    @AfterStartUp(order = AfterStartUp.REGULAR_SERVICE)
    public void afterStartUp() {
        startConsumers();
    }

    /**
     * 启动所有消费者
     */
    protected void startConsumers() {
        // 订阅消息主题
        nfConsumer.subscribe();
        // 启动消费线程
        nfConsumer.launch();
    }

    /**
     * 分区变更事件过滤（仅处理本服务类型的变更）
     */
    @Override
    protected boolean filterTbApplicationEvent(PartitionChangeEvent event) {
        return event.getServiceType() == getServiceType();
    }

    protected abstract ServiceType getServiceType();

    protected void stopConsumers() {
        nfConsumer.stop();
    }

    protected abstract long getNotificationPollDuration();

    protected abstract long getNotificationPackProcessingTimeout();

    protected abstract int getMgmtThreadPoolSize();

    protected abstract TbQueueConsumer<TbProtoQueueMsg<N>> createNotificationsConsumer();

    /**
     * 处理通知消息包（核心流程）
     * @param msgs 原始消息列表
     * @param consumer 消费者实例（用于提交偏移量）
     */
    protected void processNotifications(List<TbProtoQueueMsg<N>> msgs, TbQueueConsumer<TbProtoQueueMsg<N>> consumer) throws Exception {
        // 1. 消息包装（为每条消息分配唯一ID）
        List<IdMsgPair<N>> orderedMsgList = msgs.stream().map(msg -> new IdMsgPair<>(UUID.randomUUID(), msg)).toList();
        ConcurrentMap<UUID, TbProtoQueueMsg<N>> pendingMap = orderedMsgList.stream().collect(
                Collectors.toConcurrentMap(IdMsgPair::getUuid, IdMsgPair::getMsg));
        CountDownLatch processingTimeoutLatch = new CountDownLatch(1);
        // 2. 构建处理上下文
        TbPackProcessingContext<TbProtoQueueMsg<N>> ctx = new TbPackProcessingContext<>(
                processingTimeoutLatch, pendingMap, new ConcurrentHashMap<>());
        // 3. 并行处理消息
        orderedMsgList.forEach(element -> {
            UUID id = element.getUuid();
            TbProtoQueueMsg<N> msg = element.getMsg();
            log.trace("[{}] Creating notification callback for message: {}", id, msg.getValue());
            TbCallback callback = new TbPackCallback<>(id, ctx);
            try {
                handleNotification(id, msg, callback);
            } catch (Throwable e) {
                log.warn("[{}] Failed to process notification: {}", id, msg, e);
                callback.onFailure(e);
            }
        });
        // 4. 等待处理完成（超时机制）
        if (!processingTimeoutLatch.await(getNotificationPackProcessingTimeout(), TimeUnit.MILLISECONDS)) {
            // 记录超时消息
            ctx.getAckMap().forEach((id, msg) -> log.warn("[{}] Timeout to process notification: {}", id, msg.getValue()));
            ctx.getFailedMap().forEach((id, msg) -> log.warn("[{}] Failed to process notification: {}", id, msg.getValue()));
        }
        // 5. 提交消费偏移量
        consumer.commit();
    }

    /**
     * 处理组件生命周期事件的核心路由逻辑
     * 根据不同的实体类型执行相应的缓存失效和状态更新操作
     * @param id 消息跟踪ID（用于日志追踪）
     * @param componentLifecycleMsg 生命周期事件消息
     */
    protected final void handleComponentLifecycleMsg(UUID id, ComponentLifecycleMsg componentLifecycleMsg) {
        TenantId tenantId = componentLifecycleMsg.getTenantId();
        log.debug("[{}][{}][{}] Received Lifecycle event: {}", tenantId, componentLifecycleMsg.getEntityId().getEntityType(),
                componentLifecycleMsg.getEntityId(), componentLifecycleMsg.getEvent());
        // 按实体类型路由处理
        if (EntityType.TENANT_PROFILE.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            // 租户配置变更处理
            TenantProfileId tenantProfileId = new TenantProfileId(componentLifecycleMsg.getEntityId().getId());
            // 失效租户配置缓存
            tenantProfileCache.evict(tenantProfileId);
            // 如果是租户配置变更，并更新API使用状态（因为租户配置和api使用限流等相关）
            if (componentLifecycleMsg.getEvent().equals(ComponentLifecycleEvent.UPDATED)) {
                apiUsageStateService.onTenantProfileUpdate(tenantProfileId);
            }
        } else if (EntityType.TENANT.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            // 租户变更处理
            if (TenantId.SYS_TENANT_ID.equals(tenantId)) {
                // 如果变更的是系统租户，则重新加载JWT设置
                jwtSettingsService.reloadJwtSettings();
                return;
            } else {
                // 普通租户：失效租户缓存并失效分区中的租户缓存
                tenantProfileCache.evict(tenantId);
                partitionService.evictTenantInfo(tenantId);
                if (componentLifecycleMsg.getEvent().equals(ComponentLifecycleEvent.UPDATED)) {
                    // 如果是租户变更，则更新API使用状态
                    apiUsageStateService.onTenantUpdate(tenantId);
                } else if (componentLifecycleMsg.getEvent().equals(ComponentLifecycleEvent.DELETED)) {
                    // 如果是租户删除，则删除API使用状态并删除分区租户信息缓存
                    apiUsageStateService.onTenantDelete(tenantId);
                    partitionService.removeTenant(tenantId);
                }
            }
        } else if (EntityType.DEVICE_PROFILE.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            // 设备配置变更：失效设备配置缓存
            deviceProfileCache.evict(tenantId, new DeviceProfileId(componentLifecycleMsg.getEntityId().getId()));
        } else if (EntityType.DEVICE.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            // 设备变更：失效设备相关缓存
            deviceProfileCache.evict(tenantId, new DeviceId(componentLifecycleMsg.getEntityId().getId()));
        } else if (EntityType.ASSET_PROFILE.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            // 资产配置变更：失效资产配置缓存
            assetProfileCache.evict(tenantId, new AssetProfileId(componentLifecycleMsg.getEntityId().getId()));
        } else if (EntityType.ASSET.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            // 资产变更：失效资产相关缓存
            assetProfileCache.evict(tenantId, new AssetId(componentLifecycleMsg.getEntityId().getId()));
        } else if (EntityType.ENTITY_VIEW.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            // 实体视图变更：转发给实体视图服务处理
            actorContext.getTbEntityViewService().onComponentLifecycleMsg(componentLifecycleMsg);
        } else if (EntityType.API_USAGE_STATE.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            // API使用状态变更：更新API使用状态
            apiUsageStateService.onApiUsageStateUpdate(tenantId);
        } else if (EntityType.CUSTOMER.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            // 客户删除：清理相关API使用状态
            if (componentLifecycleMsg.getEvent() == ComponentLifecycleEvent.DELETED) {
                apiUsageStateService.onCustomerDelete((CustomerId) componentLifecycleMsg.getEntityId());
            }
        } else if (EntityType.CALCULATED_FIELD.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            // 计算字段变更：更新计算字段缓存
            if (componentLifecycleMsg.getEvent() == ComponentLifecycleEvent.CREATED) {
                calculatedFieldCache.addCalculatedField(tenantId, (CalculatedFieldId) componentLifecycleMsg.getEntityId());
            } else if (componentLifecycleMsg.getEvent() == ComponentLifecycleEvent.UPDATED) {
                calculatedFieldCache.updateCalculatedField(tenantId, (CalculatedFieldId) componentLifecycleMsg.getEntityId());
            } else {
                calculatedFieldCache.evict((CalculatedFieldId) componentLifecycleMsg.getEntityId());
            }
        }
        // 发布事件（触发其他监听器）
        eventPublisher.publishEvent(componentLifecycleMsg);
        log.trace("[{}] Forwarding component lifecycle message to App Actor {}", id, componentLifecycleMsg);
        // 转发给Actor系统处理
        actorContext.tellWithHighPriority(componentLifecycleMsg);
    }

    protected abstract void handleNotification(UUID id, TbProtoQueueMsg<N> msg, TbCallback callback) throws Exception;

    @PreDestroy
    public void destroy() {
        stopConsumers();
        if (consumersExecutor != null) {
            consumersExecutor.shutdownNow();
        }
        if (mgmtExecutor != null) {
            mgmtExecutor.shutdownNow();
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

}
