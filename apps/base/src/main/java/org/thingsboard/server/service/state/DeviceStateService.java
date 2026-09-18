package org.thingsboard.server.service.state;

import org.springframework.context.ApplicationListener;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.msg.queue.TbCallback;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.queue.discovery.event.PartitionChangeEvent;

/**
 * Created by ashvayka on 01.05.18.
 */
public interface DeviceStateService extends ApplicationListener<PartitionChangeEvent> {

    void onDeviceConnect(TenantId tenantId, DeviceId deviceId, long lastConnectTime);

    default void onDeviceConnect(TenantId tenantId, DeviceId deviceId) {
        onDeviceConnect(tenantId, deviceId, System.currentTimeMillis());
    }

    void onDeviceActivity(TenantId tenantId, DeviceId deviceId, long lastReportedActivityTime);

    void onDeviceDisconnect(TenantId tenantId, DeviceId deviceId, long lastDisconnectTime);

    default void onDeviceDisconnect(TenantId tenantId, DeviceId deviceId) {
        onDeviceDisconnect(tenantId, deviceId, System.currentTimeMillis());
    }

    void onDeviceInactivity(TenantId tenantId, DeviceId deviceId, long lastInactivityTime);

    /**
     * 传输层最后一个长连接会话已关闭。用 Core 时钟把设备标为非活跃，避免传输/Core 时钟差导致上报被丢弃。
     */
    void onLastSessionClosed(TenantId tenantId, DeviceId deviceId);

    void onDeviceInactivityTimeoutUpdate(TenantId tenantId, DeviceId deviceId, long inactivityTimeout);

    void onQueueMsg(TransportProtos.DeviceStateServiceMsgProto proto, TbCallback bytes);

}
