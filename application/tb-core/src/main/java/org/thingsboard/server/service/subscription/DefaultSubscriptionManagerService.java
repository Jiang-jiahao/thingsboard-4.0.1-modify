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
package org.thingsboard.server.service.subscription;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.thingsboard.server.cluster.TbClusterService;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.alarm.AlarmInfo;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.id.UserId;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
import org.thingsboard.server.common.data.kv.BaseAttributeKvEntry;
import org.thingsboard.server.common.data.kv.BasicTsKvEntry;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.data.kv.StringDataEntry;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.common.msg.queue.TbCallback;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.common.msg.rule.engine.DeviceAttributesEventNotificationMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToCoreNotificationMsg;
import org.thingsboard.server.queue.TbQueueProducer;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;
import org.thingsboard.server.queue.discovery.PartitionService;
import org.thingsboard.server.queue.discovery.TbApplicationEventListener;
import org.thingsboard.server.queue.discovery.TbServiceInfoProvider;
import org.thingsboard.server.queue.discovery.TopicService;
import org.thingsboard.server.queue.discovery.event.OtherServiceShutdownEvent;
import org.thingsboard.server.queue.discovery.event.PartitionChangeEvent;
import org.thingsboard.server.queue.provider.TbQueueProducerProvider;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.ws.notification.sub.NotificationUpdate;
import org.thingsboard.server.service.ws.notification.sub.NotificationsSubscriptionUpdate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


/**
 * 默认订阅管理服务实现
 * <p>
 * 作为订阅信息的管理中心，负责处理客户端的订阅请求（如订阅某个设备的属性变化）、
 * 维护所有活跃订阅的注册表，并将数据更新事件分发给正确的订阅者。
 */
@Slf4j
@TbCoreComponent
@Service
@RequiredArgsConstructor
public class DefaultSubscriptionManagerService extends TbApplicationEventListener<PartitionChangeEvent> implements SubscriptionManagerService {

    private final TopicService topicService;
    private final PartitionService partitionService;
    private final TbServiceInfoProvider serviceInfoProvider;
    private final TbQueueProducerProvider producerProvider;
    private final TbLocalSubscriptionService localSubscriptionService;
    private final TbClusterService clusterService;
    private final SubscriptionSchedulerComponent scheduler;

    /**
     * 订阅操作锁，确保对 entitySubscriptions 的线程安全访问
     */
    private final Lock subsLock = new ReentrantLock();

    /**
     * 实体远程订阅信息映射：EntityId -> TbEntityRemoteSubsInfo
     */
    private final ConcurrentMap<EntityId, TbEntityRemoteSubsInfo> entitySubscriptions = new ConcurrentHashMap<>();

    /**
     * 实体更新信息缓存：EntityId -> TbEntityUpdatesInfo
     * <p>
     * 记录每个实体的最后更新时间，用于错过更新补偿
     */
    private final ConcurrentMap<EntityId, TbEntityUpdatesInfo> entityUpdates = new ConcurrentHashMap<>();

    private String serviceId;

    private TbQueueProducer<TbProtoQueueMsg<ToCoreNotificationMsg>> toCoreNotificationsProducer;

    private long initTs;

    @PostConstruct
    public void initExecutor() {
        serviceId = serviceInfoProvider.getServiceId();
        initTs = System.currentTimeMillis();
        toCoreNotificationsProducer = producerProvider.getTbCoreNotificationsMsgProducer();
        scheduler.scheduleWithFixedDelay(this::cleanupEntityUpdates, 1, 1, TimeUnit.HOURS);
    }

    /**
     * 处理订阅事件（来自其他服务节点）
     * <p>
     * 当其他服务节点的订阅状态发生变化时，会调用此方法
     *
     * @param serviceId 发送事件的服务ID
     * @param event 订阅事件
     * @param callback 回调接口
     */
    @Override
    public void onSubEvent(String serviceId, TbEntitySubEvent event, TbCallback callback) {
        var tenantId = event.getTenantId();
        var entityId = event.getEntityId();
        log.trace("[{}][{}][{}] Processing subscription event {}", tenantId, entityId, serviceId, event);
        // 解析实体所在的分区
        TopicPartitionInfo tpi = partitionService.resolve(ServiceType.TB_CORE, tenantId, entityId);
        if (tpi.isMyPartition()) {
            // 实体属于当前分区，处理订阅事件
            subsLock.lock();
            try {
                var entitySubs = entitySubscriptions.computeIfAbsent(entityId, id -> new TbEntityRemoteSubsInfo(tenantId, entityId));
                // 更新订阅状态并检查是否为空
                boolean empty = entitySubs.updateAndCheckIsEmpty(serviceId, event);
                if (empty) {
                    entitySubscriptions.remove(entityId);
                }
            } finally {
                subsLock.unlock();
            }
            callback.onSuccess();
            // 如果事件包含时间序列或属性订阅，发送回调确认
            if (event.hasTsOrAttrSub()) {
                sendSubEventCallback(tenantId, serviceId, entityId, event.getSeqNumber());
            }
        } else {
            // 实体不属于当前分区，可能是重新平衡过程中
            log.warn("[{}][{}][{}] Event belongs to external partition. Probably re-balancing is in progress. Topic: {}"
                    , tenantId, entityId, serviceId, tpi.getFullTopicName());
            callback.onFailure(new RuntimeException("Entity belongs to external partition " + tpi.getFullTopicName() + "!"));
        }
    }

    /**
     * 处理其他服务关闭事件（该监听和PartitionChangeEvent应可以合一个）
     * <p>
     * 当其他 TB_CORE 服务节点关闭时，清理其相关的订阅信息
     *
     * @param event 服务关闭事件
     */
    @Override
    @EventListener(OtherServiceShutdownEvent.class)
    public void onApplicationEvent(OtherServiceShutdownEvent event) {
        if (event.getServiceTypes() != null && event.getServiceTypes().contains(ServiceType.TB_CORE)) {
            subsLock.lock();
            try {
                int sizeBeforeCleanup = entitySubscriptions.size();
                entitySubscriptions.entrySet().removeIf(kv -> kv.getValue().removeAndCheckIsEmpty(event.getServiceId()));
                log.info("[{}][{}] Removed {} entity subscription records due to server shutdown.", serviceId, event.getServiceId(), entitySubscriptions.size() - sizeBeforeCleanup);
            } finally {
                subsLock.unlock();
            }
        }
    }

    private void sendSubEventCallback(TenantId tenantId, String targetId, EntityId entityId, int seqNumber) {
        var update = getEntityUpdatesInfo(entityId);
        if (serviceId.equals(targetId)) {
            localSubscriptionService.onSubEventCallback(tenantId, entityId, seqNumber, update, TbCallback.EMPTY);
        } else {
            sendCoreNotification(targetId, entityId, TbSubscriptionUtils.toProto(tenantId, entityId.getId(), seqNumber, update));
        }
    }

    @Override
    protected void onTbApplicationEvent(PartitionChangeEvent partitionChangeEvent) {
        if (ServiceType.TB_CORE.equals(partitionChangeEvent.getServiceType())) {
            entitySubscriptions.values().removeIf(sub ->
                    !partitionService.isMyPartition(ServiceType.TB_CORE, sub.getTenantId(), sub.getEntityId()));
        }
    }

    @Override
    public void onTimeSeriesUpdate(TenantId tenantId, EntityId entityId, List<TsKvEntry> ts, TbCallback callback) {
        onTimeSeriesUpdate(entityId, ts);
        callback.onSuccess();
    }

    @Override
    public void onTimeSeriesDelete(TenantId tenantId, EntityId entityId, List<String> keys, TbCallback callback) {
        onTimeSeriesUpdate(entityId,
                keys.stream().map(key -> new BasicTsKvEntry(0, new StringDataEntry(key, ""))).collect(Collectors.toList()));
        callback.onSuccess();
    }

    private void onTimeSeriesUpdate(EntityId entityId, List<TsKvEntry> update) {
        getEntityUpdatesInfo(entityId).timeSeriesUpdateTs = System.currentTimeMillis();
        TbEntityRemoteSubsInfo subInfo = entitySubscriptions.get(entityId);
        if (subInfo != null) {
            log.trace("[{}] Handling time-series update: {}", entityId, update);
            subInfo.getSubs().forEach((serviceId, sub) -> {
                if (sub.tsAllKeys) {
                    onTimeSeriesUpdate(serviceId, entityId, update);
                } else if (sub.tsKeys != null) {
                    List<TsKvEntry> tmp = getSubList(update, sub.tsKeys);
                    if (tmp != null) {
                        onTimeSeriesUpdate(serviceId, entityId, tmp);
                    }
                }
            });
        } else {
            log.trace("[{}] No time-series subscriptions for entity.", entityId);
        }
    }

    private void onTimeSeriesUpdate(String targetId, EntityId entityId, List<TsKvEntry> update) {
        if (serviceId.equals(targetId)) {
            localSubscriptionService.onTimeSeriesUpdate(entityId, update, TbCallback.EMPTY);
        } else {
            sendCoreNotification(targetId, entityId, TbSubscriptionUtils.toProto(entityId, update));
        }
    }

    @Override
    public void onAttributesUpdate(TenantId tenantId, EntityId entityId, String scope, List<AttributeKvEntry> attributes, TbCallback callback) {
        getEntityUpdatesInfo(entityId).attributesUpdateTs = System.currentTimeMillis();
        processAttributesUpdate(entityId, scope, attributes);
        callback.onSuccess();
    }

    @Override
    public void onAttributesDelete(TenantId tenantId, EntityId entityId, String scope, List<String> keys, TbCallback callback) {
        onAttributesDelete(tenantId, entityId, scope, keys, false, callback);
    }

    @Override
    public void onAttributesDelete(TenantId tenantId, EntityId entityId, String scope, List<String> keys, boolean notifyDevice, TbCallback callback) {
        processAttributesUpdate(entityId, scope,
                keys.stream().map(key -> new BaseAttributeKvEntry(0, new StringDataEntry(key, ""))).collect(Collectors.toList()));
        if (entityId.getEntityType() == EntityType.DEVICE && TbAttributeSubscriptionScope.SHARED_SCOPE.name().equalsIgnoreCase(scope) && notifyDevice) {
            clusterService.pushMsgToCore(DeviceAttributesEventNotificationMsg.onDelete(tenantId, new DeviceId(entityId.getId()), scope, keys), null);
        }
        callback.onSuccess();
    }

    private void processAttributesUpdate(EntityId entityId, String scope, List<AttributeKvEntry> update) {
        TbEntityRemoteSubsInfo subInfo = entitySubscriptions.get(entityId);
        if (subInfo != null) {
            log.trace("[{}] Handling attributes update: {}", entityId, update);
            subInfo.getSubs().forEach((serviceId, sub) -> {
                if (sub.attrAllKeys) {
                    processAttributesUpdate(serviceId, entityId, scope, update);
                } else if (sub.attrKeys != null) {
                    List<AttributeKvEntry> tmp = getSubList(update, sub.attrKeys);
                    if (tmp != null) {
                        processAttributesUpdate(serviceId, entityId, scope, tmp);
                    }
                }
            });
        } else {
            log.trace("[{}] No attributes subscriptions for entity.", entityId);
        }
    }

    private void processAttributesUpdate(String targetId, EntityId entityId, String scope, List<AttributeKvEntry> update) {
        List<TsKvEntry> tsKvEntryList = update.stream().map(attr -> new BasicTsKvEntry(attr.getLastUpdateTs(), attr)).collect(Collectors.toList());
        if (serviceId.equals(targetId)) {
            localSubscriptionService.onAttributesUpdate(entityId, scope, tsKvEntryList, TbCallback.EMPTY);
        } else {
            sendCoreNotification(targetId, entityId, TbSubscriptionUtils.toProto(scope, entityId, tsKvEntryList));
        }
    }

    @Override
    public void onAlarmUpdate(TenantId tenantId, EntityId entityId, AlarmInfo alarm, TbCallback callback) {
        onAlarmSubUpdate(tenantId, entityId, alarm, false, callback);
    }

    @Override
    public void onAlarmDeleted(TenantId tenantId, EntityId entityId, AlarmInfo alarm, TbCallback callback) {
        onAlarmSubUpdate(tenantId, entityId, alarm, true, callback);
    }

    private void onAlarmSubUpdate(TenantId tenantId, EntityId entityId, AlarmInfo alarm, boolean deleted, TbCallback callback) {
        TbEntityRemoteSubsInfo subInfo = entitySubscriptions.get(entityId);
        if (subInfo != null) {
            log.trace("[{}][{}] Handling alarm update {}: {}", tenantId, entityId, alarm, deleted);
            for (Map.Entry<String, TbSubscriptionsInfo> entry : subInfo.getSubs().entrySet()) {
                if (entry.getValue().alarms) {
                    onAlarmSubUpdate(entry.getKey(), entityId, alarm, deleted);
                }
            }
        }
        callback.onSuccess();
    }

    private void onAlarmSubUpdate(String targetServiceId, EntityId entityId, AlarmInfo alarm, boolean deleted) {
        if (alarm == null) {
            log.warn("[{}] empty alarm update!", entityId);
            return;
        }
        if (serviceId.equals(targetServiceId)) {
            log.trace("[{}] Forwarding to local service: {} deleted: {}", entityId, alarm, deleted);
            localSubscriptionService.onAlarmUpdate(entityId, alarm, deleted, TbCallback.EMPTY);
        } else {
            sendCoreNotification(targetServiceId, entityId,
                    TbSubscriptionUtils.toAlarmSubUpdateToProto(entityId, alarm, deleted));
        }
    }

    private void sendCoreNotification(String targetServiceId, EntityId entityId, ToCoreNotificationMsg msg) {
        log.trace("[{}] Forwarding to remote service [{}]: {}", entityId, targetServiceId, msg);
        TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.TB_CORE, targetServiceId);
        TbProtoQueueMsg<ToCoreNotificationMsg> queueMsg = new TbProtoQueueMsg<>(entityId.getId(), msg);
        toCoreNotificationsProducer.send(tpi, queueMsg, null);
    }

    @Override
    public void onNotificationUpdate(TenantId tenantId, UserId entityId, NotificationUpdate notificationUpdate, TbCallback callback) {
        TbEntityRemoteSubsInfo subInfo = entitySubscriptions.get(entityId);
        if (subInfo != null) {
            NotificationsSubscriptionUpdate subscriptionUpdate = new NotificationsSubscriptionUpdate(notificationUpdate);
            log.trace("[{}][{}] Handling notificationUpdate for user {}", tenantId, entityId, notificationUpdate);
            for (Map.Entry<String, TbSubscriptionsInfo> entry : subInfo.getSubs().entrySet()) {
                if (entry.getValue().notifications) {
                    onNotificationsSubUpdate(entry.getKey(), entityId, subscriptionUpdate);
                }
            }
        }
        callback.onSuccess();
    }

    private void onNotificationsSubUpdate(String targetServiceId, EntityId entityId, NotificationsSubscriptionUpdate subscriptionUpdate) {
        if (serviceId.equals(targetServiceId)) {
            log.trace("[{}] Forwarding to local service: {}", entityId, subscriptionUpdate);
            localSubscriptionService.onNotificationUpdate(entityId, subscriptionUpdate, TbCallback.EMPTY);
        } else {
            sendCoreNotification(targetServiceId, entityId,
                    TbSubscriptionUtils.notificationsSubUpdateToProto(entityId, subscriptionUpdate));
        }
    }

    private static <T extends KvEntry> List<T> getSubList(List<T> ts, Set<String> keys) {
        List<T> update = null;
        for (T entry : ts) {
            if (keys.contains(entry.getKey())) {
                if (update == null) {
                    update = new ArrayList<>(ts.size());
                }
                update.add(entry);
            }
        }
        return update;
    }

    private TbEntityUpdatesInfo getEntityUpdatesInfo(EntityId entityId) {
        return entityUpdates.computeIfAbsent(entityId, id -> new TbEntityUpdatesInfo(initTs));
    }

    private void cleanupEntityUpdates() {
        initTs = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        int sizeBeforeCleanup = entityUpdates.size();
        entityUpdates.entrySet().removeIf(kv -> {
            var v = kv.getValue();
            return initTs > v.attributesUpdateTs && initTs > v.timeSeriesUpdateTs;
        });
        log.info("Removed {} old entity update records.", entityUpdates.size() - sizeBeforeCleanup);
    }

}
