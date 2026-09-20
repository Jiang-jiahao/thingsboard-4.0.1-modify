package com.jnks.iot.server.common.transport;

import lombok.Getter;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.queue.discovery.event.TbApplicationEvent;

public final class DeviceDeletedEvent extends TbApplicationEvent {

    private static final long serialVersionUID = -7453664970966733857L;
    @Getter
    private final DeviceId deviceId;

    public DeviceDeletedEvent(DeviceId deviceId) {
        super(new Object());
        this.deviceId = deviceId;
    }
}
