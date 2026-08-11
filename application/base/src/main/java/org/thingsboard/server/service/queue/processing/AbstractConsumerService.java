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
import java.util.Optional;
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
 * 各服务节点「通知队列」消费与公共基础设施的抽象基类。
 * <p>
 * 本类不负责主业务队列（如 Core 的 {@code ToCoreMsg}、Rule Engine 的规则队列）的分区消费，
 * 那些由子类自行用 {@link org.thingsboard.server.queue.common.consumer.MainQueueConsumerManager}
 * 等组件管理。本类统一提供：
 * <ul>
 *   <li>消费/管理/调度三类线程池；</li>
 *   <li>本节点专属的通知 Topic 消费者 {@link #nfConsumer}（消息类型由泛型 {@code N} 决定）；</li>
 *   <li>通知消息的批处理框架（pending map + 超时 latch + commit）；</li>
 *   <li>对 {@link PartitionChangeEvent} 的监听基座（按 {@link #getServiceType()} 过滤）；</li>
 *   <li>组件生命周期消息的公共缓存失效与 Actor 转发逻辑。</li>
 * </ul>
 * 典型子类：{@code DefaultTbCoreConsumerService}、{@code DefaultTbRuleEngineConsumerService} 等。
 * 生命周期：子类 {@code @PostConstruct} 调用 {@link #init(String)} → 应用就绪后
 * {@link #afterStartUp()} 启动通知消费者 → {@link #destroy()} 停止并关闭线程池。
 * 分区变更的具体订阅更新由子类覆盖 {@code onTbApplicationEvent} 完成。
 *
 * @param <N> 通知队列中的 Protobuf 消息类型（如 {@code ToCoreNotificationMsg}）
 * @see QueueConsumerManager
 * @see PartitionChangeEvent
 */
@RequiredArgsConstructor
public abstract class AbstractConsumerService<N extends com.google.protobuf.GeneratedMessageV3> extends TbApplicationEventListener<PartitionChangeEvent> {

    /** 使用运行时子类名作为 logger 名，便于区分 Core / RE 等实现的日志 */
    protected final Logger log = LoggerFactory.getLogger(getClass());

    /** Actor 系统上下文：转发高优先级生命周期消息、访问 EntityView 等扩展服务 */
    protected final ActorSystemContext actorContext;

    /** 租户配置缓存：租户/租户配置变更时驱逐 */
    protected final TbTenantProfileCache tenantProfileCache;

    /** 设备配置缓存：设备或设备配置变更时驱逐 */
    protected final TbDeviceProfileCache deviceProfileCache;

    /** 资产配置缓存：资产或资产配置变更时驱逐 */
    protected final TbAssetProfileCache assetProfileCache;

    /** 计算字段缓存：计算字段增删改时同步维护 */
    protected final CalculatedFieldCache calculatedFieldCache;

    /** API 用量状态：租户/配置/客户变更时刷新或清理用量相关状态 */
    protected final TbApiUsageStateService apiUsageStateService;

    /** 分区与租户路由服务：驱逐租户路由、删除租户分区信息等 */
    protected final PartitionService partitionService;

    /** Spring 事件发布器：将组件生命周期消息再广播给本进程内其它监听者 */
    protected final ApplicationEventPublisher eventPublisher;

    /**
     * JWT 设置服务（可选）。
     * <p>
     * 系统租户上的租户实体生命周期事件会触发重新加载 JWT 配置；
     * 未启用 JWT 动态配置时为空。
     */
    protected final Optional<JwtSettingsService> jwtSettingsService;

    /**
     * 本服务类型的通知队列消费者管理器。
     * <p>
     * 订阅的是「按 serviceId 区分的通知 Topic」（点对点通知），与按实体哈希分区的主业务 Topic 不同。
     * 在 {@link #init(String)} 中构建，在 {@link #startConsumers()} 中 subscribe + launch。
     */
    protected QueueConsumerManager<TbProtoQueueMsg<N>> nfConsumer;

    /**
     * 消息消费与批内提交逻辑使用的线程池（CachedThreadPool）。
     * <p>
     * 同时作为通知消费者的 {@code consumerExecutor}；子类主通道消费者通常也复用此池。
     */
    protected ExecutorService consumersExecutor;

    /**
     * 管理类任务线程池（WorkStealingPool）。
     * <p>
     * 供子类交给 {@code MainQueueConsumerManager} 的 {@code taskExecutor}，
     * 用于处理配置/分区等管理任务，与 poll 循环线程分离。大小由 {@link #getMgmtThreadPoolSize()} 决定。
     */
    protected ExecutorService mgmtExecutor;

    /**
     * 单线程调度器。
     * <p>
     * 供子类消费者管理器在抢锁失败时延迟重试等场景使用。
     */
    protected ScheduledExecutorService scheduler;

    /**
     * 初始化公共线程池与通知消费者管理器。
     * <p>
     * 由子类在 {@code @PostConstruct} 中调用。此处只 build 消费者，不启动；
     * 真正 subscribe/launch 在 {@link #afterStartUp()} → {@link #startConsumers()}。
     *
     * @param prefix 线程名前缀（如 {@code "tb-core"}），实际线程名形如 {@code prefix-consumer} /
     *               {@code prefix-mgmt} / {@code prefix-consumer-scheduler}
     */
    public void init(String prefix) {
        this.consumersExecutor = Executors.newCachedThreadPool(ThingsBoardThreadFactory.forName(prefix + "-consumer"));
        this.mgmtExecutor = ThingsBoardExecutors.newWorkStealingPool(getMgmtThreadPoolSize(), prefix + "-mgmt");
        this.scheduler = ThingsBoardExecutors.newSingleThreadScheduledExecutor(prefix + "-consumer-scheduler");

        this.nfConsumer = QueueConsumerManager.<TbProtoQueueMsg<N>>builder()
                .name(getServiceType().getLabel() + " Notifications")
                .msgPackProcessor(this::processNotifications)
                .pollInterval(getNotificationPollDuration())
                .consumerCreator(this::createNotificationsConsumer)
                .consumerExecutor(consumersExecutor)
                .threadPrefix("notifications")
                .build();
    }

    /**
     * 应用启动完成后启动消费者。
     * <p>
     * 使用 {@link AfterStartUp#REGULAR_SERVICE} 顺序，保证分区发现等更靠前的启动阶段已完成后再拉通知队列。
     * 子类可覆盖 {@link #startConsumers()} 在调用 {@code super} 之外再启动主通道、用量统计等消费者。
     */
    @AfterStartUp(order = AfterStartUp.REGULAR_SERVICE)
    public void afterStartUp() {
        startConsumers();
    }

    /**
     * 启动通知消费者：先订阅通知 Topic，再启动 poll 循环。
     * <p>
     * 默认只处理 {@link #nfConsumer}。子类通常 override 并 {@code super.startConsumers()} 后
     * 再启动自身其它 {@code QueueConsumerManager}。
     */
    protected void startConsumers() {
        nfConsumer.subscribe();
        nfConsumer.launch();
    }

    /**
     * 只处理与本服务类型匹配的分区变更事件。
     * <p>
     * 例如 Core 子类 {@code getServiceType() == TB_CORE} 时，忽略 Rule Engine 的
     * {@link PartitionChangeEvent}，避免无关重订阅。
     *
     * @param event 分区变更事件
     * @return {@code true} 表示事件应交给本监听器后续处理
     */
    @Override
    protected boolean filterTbApplicationEvent(PartitionChangeEvent event) {
        return event.getServiceType() == getServiceType();
    }

    /**
     * @return 本消费者服务所属的服务类型，用于过滤分区事件、命名通知消费者等
     */
    protected abstract ServiceType getServiceType();

    /**
     * 停止通知消费者。
     * <p>
     * 子类 override 时应先或后调用 {@code super.stopConsumers()}，并停止自身持有的其它消费者。
     */
    protected void stopConsumers() {
        nfConsumer.stop();
    }

    /**
     * @return 通知队列 poll 间隔（毫秒）
     */
    protected abstract long getNotificationPollDuration();

    /**
     * @return 单批通知消息处理的最长等待时间（毫秒）；超时后仍会 commit offset
     */
    protected abstract long getNotificationPackProcessingTimeout();

    /**
     * @return {@link #mgmtExecutor} 的并行度
     */
    protected abstract int getMgmtThreadPoolSize();

    /**
     * 创建底层通知队列消费者实例（Topic / group 由各服务的 QueueFactory 决定）。
     *
     * @return 可被 {@link QueueConsumerManager} 托管的消费者
     */
    protected abstract TbQueueConsumer<TbProtoQueueMsg<N>> createNotificationsConsumer();

    /**
     * 通知队列一批消息的通用处理模板。
     * <p>
     * 流程：
     * <ol>
     *   <li>为每条消息分配 UUID，构建 pending 映射与 {@link TbPackProcessingContext}；</li>
     *   <li>逐条调用 {@link #handleNotification}，通过 {@link TbPackCallback} 汇总成功/失败；</li>
     *   <li>等待整批完成，最长 {@link #getNotificationPackProcessingTimeout()}；</li>
     *   <li>超时则打印仍未完成或已失败的消息；</li>
     *   <li>无论是否超时，最后 {@code consumer.commit()}。</li>
     * </ol>
     * 与主通道批处理的区别：此处在当前消费线程内同步提交各条通知，不再另起
     * {@code consumersExecutor.submit} 包一层（具体异步由子类 {@code handleNotification} 自行决定）。
     *
     * @param msgs     本批通知消息
     * @param consumer 拉取本批消息的消费者，用于 commit
     * @throws Exception 等待 latch 被中断等场景可能抛出
     */
    protected void processNotifications(List<TbProtoQueueMsg<N>> msgs, TbQueueConsumer<TbProtoQueueMsg<N>> consumer) throws Exception {
        List<IdMsgPair<N>> orderedMsgList = msgs.stream().map(msg -> new IdMsgPair<>(UUID.randomUUID(), msg)).toList();
        ConcurrentMap<UUID, TbProtoQueueMsg<N>> pendingMap = orderedMsgList.stream().collect(
                Collectors.toConcurrentMap(IdMsgPair::getUuid, IdMsgPair::getMsg));
        CountDownLatch processingTimeoutLatch = new CountDownLatch(1);
        TbPackProcessingContext<TbProtoQueueMsg<N>> ctx = new TbPackProcessingContext<>(
                processingTimeoutLatch, pendingMap, new ConcurrentHashMap<>());
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
        if (!processingTimeoutLatch.await(getNotificationPackProcessingTimeout(), TimeUnit.MILLISECONDS)) {
            ctx.getAckMap().forEach((id, msg) -> log.warn("[{}] Timeout to process notification: {}", id, msg.getValue()));
            ctx.getFailedMap().forEach((id, msg) -> log.warn("[{}] Failed to process notification: {}", id, msg.getValue()));
        }
        consumer.commit();
    }

    /**
     * 处理组件生命周期消息：按实体类型刷新本地缓存 / 用量状态 / 分区租户信息，
     * 再对本进程发布 Spring 事件，并以高优先级投递到 App Actor。
     * <p>
     * 各实体类型要点：
     * <ul>
     *   <li>租户配置：驱逐缓存；UPDATED 时通知 API 用量；</li>
     *   <li>系统租户实体：重新加载 JWT 设置后直接返回（不再走后续 Actor 转发前的租户分支逻辑）；</li>
     *   <li>普通租户：驱逐配置与路由缓存；UPDATED/DELETED 时更新或删除用量，DELETED 时移除租户分区信息；</li>
     *   <li>设备/资产及其 Profile：驱逐对应缓存；</li>
     *   <li>Entity View：委托 {@code TbEntityViewService}（若存在）；</li>
     *   <li>API 用量状态、客户删除、计算字段增删改：更新对应服务或缓存。</li>
     * </ul>
     * 子类在 {@link #handleNotification} 中解析出生命周期协议后，通常调用本方法完成公共副作用。
     *
     * @param id                     当前通知处理 ID（日志关联）
     * @param componentLifecycleMsg  已反序列化的生命周期消息
     */
    protected final void handleComponentLifecycleMsg(UUID id, ComponentLifecycleMsg componentLifecycleMsg) {
        TenantId tenantId = componentLifecycleMsg.getTenantId();
        log.debug("[{}][{}][{}] Received Lifecycle event: {}", tenantId, componentLifecycleMsg.getEntityId().getEntityType(),
                componentLifecycleMsg.getEntityId(), componentLifecycleMsg.getEvent());
        if (EntityType.TENANT_PROFILE.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            TenantProfileId tenantProfileId = new TenantProfileId(componentLifecycleMsg.getEntityId().getId());
            tenantProfileCache.evict(tenantProfileId);
            if (componentLifecycleMsg.getEvent().equals(ComponentLifecycleEvent.UPDATED)) {
                apiUsageStateService.onTenantProfileUpdate(tenantProfileId);
            }
        } else if (EntityType.TENANT.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            if (TenantId.SYS_TENANT_ID.equals(tenantId)) {
                jwtSettingsService.ifPresent(JwtSettingsService::reloadJwtSettings);
                return;
            } else {
                tenantProfileCache.evict(tenantId);
                partitionService.evictTenantInfo(tenantId);
                if (componentLifecycleMsg.getEvent().equals(ComponentLifecycleEvent.UPDATED)) {
                    apiUsageStateService.onTenantUpdate(tenantId);
                } else if (componentLifecycleMsg.getEvent().equals(ComponentLifecycleEvent.DELETED)) {
                    apiUsageStateService.onTenantDelete(tenantId);
                    partitionService.removeTenant(tenantId);
                }
            }
        } else if (EntityType.DEVICE_PROFILE.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            deviceProfileCache.evict(tenantId, new DeviceProfileId(componentLifecycleMsg.getEntityId().getId()));
        } else if (EntityType.DEVICE.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            deviceProfileCache.evict(tenantId, new DeviceId(componentLifecycleMsg.getEntityId().getId()));
        } else if (EntityType.ASSET_PROFILE.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            assetProfileCache.evict(tenantId, new AssetProfileId(componentLifecycleMsg.getEntityId().getId()));
        } else if (EntityType.ASSET.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            assetProfileCache.evict(tenantId, new AssetId(componentLifecycleMsg.getEntityId().getId()));
        } else if (EntityType.ENTITY_VIEW.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            if (actorContext.getTbEntityViewService() != null) {
                actorContext.getTbEntityViewService().onComponentLifecycleMsg(componentLifecycleMsg);
            }
        } else if (EntityType.API_USAGE_STATE.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            apiUsageStateService.onApiUsageStateUpdate(tenantId);
        } else if (EntityType.CUSTOMER.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            if (componentLifecycleMsg.getEvent() == ComponentLifecycleEvent.DELETED) {
                apiUsageStateService.onCustomerDelete((CustomerId) componentLifecycleMsg.getEntityId());
            }
        } else if (EntityType.CALCULATED_FIELD.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            if (componentLifecycleMsg.getEvent() == ComponentLifecycleEvent.CREATED) {
                calculatedFieldCache.addCalculatedField(tenantId, (CalculatedFieldId) componentLifecycleMsg.getEntityId());
            } else if (componentLifecycleMsg.getEvent() == ComponentLifecycleEvent.UPDATED) {
                calculatedFieldCache.updateCalculatedField(tenantId, (CalculatedFieldId) componentLifecycleMsg.getEntityId());
            } else {
                calculatedFieldCache.evict((CalculatedFieldId) componentLifecycleMsg.getEntityId());
            }
        }
        eventPublisher.publishEvent(componentLifecycleMsg);
        log.trace("[{}] Forwarding component lifecycle message to App Actor {}", id, componentLifecycleMsg);
        actorContext.tellWithHighPriority(componentLifecycleMsg);
    }

    /**
     * 由子类实现：将单条通知消息路由到具体业务处理，并在适当时机触发 {@code callback}。
     *
     * @param id       本条消息在批处理中的关联 ID
     * @param msg      队列封装的通知协议消息
     * @param callback 成功/失败回调，驱动 {@link TbPackProcessingContext} 完成计数
     * @throws Exception 处理失败且未自行调用 {@code callback.onFailure} 时可由框架捕获
     */
    protected abstract void handleNotification(UUID id, TbProtoQueueMsg<N> msg, TbCallback callback) throws Exception;

    /**
     * Bean 销毁：停止通知消费者并立即关闭三类线程池。
     * <p>
     * 子类若持有额外消费者，应覆盖本方法或确保其 {@link #stopConsumers()} 实现中已全部停止。
     */
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
