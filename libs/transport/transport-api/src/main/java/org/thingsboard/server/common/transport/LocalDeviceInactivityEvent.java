package org.thingsboard.server.common.transport;

import lombok.Getter;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.queue.discovery.event.TbApplicationEvent;

/**
 * Same-JVM inactivity signal so monolith Core can persist {@code active=false}
 * before in-memory queues disappear on restart.
 */
public final class LocalDeviceInactivityEvent extends TbApplicationEvent {

    private static final long serialVersionUID = 1L;

    @Getter
    private final TenantId tenantId;
    @Getter
    private final DeviceId deviceId;

    public LocalDeviceInactivityEvent(TenantId tenantId, DeviceId deviceId) {
        super(deviceId);
        this.tenantId = tenantId;
        this.deviceId = deviceId;
    }
}
