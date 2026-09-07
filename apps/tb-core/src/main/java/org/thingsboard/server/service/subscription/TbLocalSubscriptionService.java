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

import org.thingsboard.server.common.data.alarm.AlarmInfo;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.msg.queue.TbCallback;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.queue.discovery.event.ClusterTopologyChangeEvent;
import org.thingsboard.server.service.ws.WebSocketSessionRef;
import org.thingsboard.server.service.ws.notification.sub.NotificationRequestUpdate;
import org.thingsboard.server.service.ws.notification.sub.NotificationsSubscriptionUpdate;

import java.util.List;

/**
 * 本机 WebSocket 订阅服务。
 * <p>
 * 管理当前 Core 节点上的会话订阅：注册/取消、接收订阅管理器推送的时序/属性/告警/通知更新并回调到 WS。
 * 本节点不负责的实体会经队列转发给对应 Core 的 {@link SubscriptionManagerService}。
 *
 * @see DefaultTbLocalSubscriptionService
 */
public interface TbLocalSubscriptionService {

    /**
     * 为会话添加订阅（校验频控后登记并推送到订阅管理器）。
     */
    void addSubscription(TbSubscription<?> subscription, WebSocketSessionRef sessionRef);

    /**
     * 处理订阅事件回调（协议版本）。
     */
    void onSubEventCallback(TransportProtos.TbEntitySubEventCallbackProto subEventCallback, TbCallback callback);

    /**
     * 处理订阅事件回调：更新实体时间戳并补偿错过的更新。
     */
    void onSubEventCallback(TenantId tenantId, EntityId entityId, int seqNumber, TbEntityUpdatesInfo entityUpdatesInfo, TbCallback empty);

    /**
     * 取消会话中指定订阅。
     */
    void cancelSubscription(TenantId tenantId, String sessionId, int subscriptionId);

    /**
     * 取消会话全部订阅。
     */
    void cancelAllSessionSubscriptions(TenantId tenantId, String sessionId);

    /**
     * 处理时序更新（协议版本）。
     */
    void onTimeSeriesUpdate(TransportProtos.TbSubUpdateProto tsUpdate, TbCallback callback);

    /**
     * 将时序更新分发给匹配的本机订阅。
     */
    void onTimeSeriesUpdate(EntityId entityId, List<TsKvEntry> update, TbCallback callback);

    /**
     * 处理属性更新（协议版本）。
     */
    void onAttributesUpdate(TransportProtos.TbSubUpdateProto attrUpdate, TbCallback callback);

    /**
     * 将属性更新分发给匹配 scope/键的本机订阅。
     */
    void onAttributesUpdate(EntityId entityId, String scope, List<TsKvEntry> update, TbCallback callback);

    /**
     * 将告警更新分发给本机告警订阅。
     */
    void onAlarmUpdate(EntityId entityId, AlarmInfo alarm, boolean deleted, TbCallback callback);

    /**
     * 处理告警更新（协议版本）。
     */
    void onAlarmUpdate(TransportProtos.TbAlarmSubUpdateProto update, TbCallback callback);

    /**
     * 将通知更新分发给本机通知订阅。
     */
    void onNotificationUpdate(EntityId entityId, NotificationsSubscriptionUpdate subscriptionUpdate, TbCallback callback);

    /**
     * 集群拓扑变化时重新对齐本机订阅到正确的 Core 分区。
     */
    void onApplicationEvent(ClusterTopologyChangeEvent event);

    /**
     * 其它 Core 启动时，把迁出分区上的订阅推给新节点。
     */
    void onCoreStartupMsg(TransportProtos.CoreStartupMsg coreStartupMsg);

    /**
     * 通知请求更新（如已读）广播给租户下用户通知订阅。
     */
    void onNotificationRequestUpdate(TenantId tenantId, NotificationRequestUpdate update, TbCallback callback);

    /**
     * 处理通知更新（协议版本）。
     */
    void onNotificationUpdate(TransportProtos.NotificationsSubUpdateProto notificationsUpdate, TbCallback callback);

}
