package com.jnks.iot.server.service.subscription;

import org.springframework.context.ApplicationListener;
import com.jnks.iot.server.common.data.alarm.AlarmInfo;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;
import com.jnks.iot.server.common.data.kv.TsKvEntry;
import com.jnks.iot.server.common.msg.queue.JnksIotCallback;
import com.jnks.iot.server.queue.discovery.event.OtherServiceShutdownEvent;
import com.jnks.iot.server.queue.discovery.event.PartitionChangeEvent;
import com.jnks.iot.server.service.ws.notification.sub.NotificationUpdate;

import java.util.List;

/**
 * 订阅管理服务
 * <p>
 * 负责处理各种实时数据订阅和推送功能。
 */
public interface SubscriptionManagerService extends ApplicationListener<PartitionChangeEvent> {

    /**
     * 处理实体订阅事件
     * 当客户端订阅设备数据时调用
     */
    void onSubEvent(String serviceId, JnksIotEntitySubEvent event, JnksIotCallback empty);

    void onApplicationEvent(OtherServiceShutdownEvent event);

    /**
     * 处理时序数据更新
     * 当设备上报新的遥测数据时调用
     *
     * @param tenantId 租户ID
     * @param entityId 实体ID（设备、资产等）
     * @param ts 时序数据列表
     * @param callback 回调函数
     */
    void onTimeSeriesUpdate(TenantId tenantId, EntityId entityId, List<TsKvEntry> ts, JnksIotCallback callback);

    /**
     * 处理属性更新
     * 当设备属性发生变化时调用
     *
     * @param scope 属性作用域（SERVER_SCOPE, SHARED_SCOPE, CLIENT_SCOPE）
     */
    void onAttributesUpdate(TenantId tenantId, EntityId entityId, String scope, List<AttributeKvEntry> attributes, JnksIotCallback callback);

    void onAttributesDelete(TenantId tenantId, EntityId entityId, String scope, List<String> keys, JnksIotCallback empty);

    /**
     * This method is retained solely for backwards compatibility, specifically to handle
     * legacy proto messages that include the notifyDevice field.
     *
     * @deprecated as of 4.0, this method will be removed in future releases.
     */
    @Deprecated(forRemoval = true, since = "4.0")
    void onAttributesDelete(TenantId tenantId, EntityId entityId, String scope, List<String> keys, boolean notifyDevice, JnksIotCallback empty);

    void onTimeSeriesDelete(TenantId tenantId, EntityId entityId, List<String> keys, JnksIotCallback callback);

    void onAlarmUpdate(TenantId tenantId, EntityId entityId, AlarmInfo alarm, JnksIotCallback callback);

    void onAlarmDeleted(TenantId tenantId, EntityId entityId, AlarmInfo alarm, JnksIotCallback callback);

    void onNotificationUpdate(TenantId tenantId, UserId recipientId, NotificationUpdate notificationUpdate, JnksIotCallback callback);

}
