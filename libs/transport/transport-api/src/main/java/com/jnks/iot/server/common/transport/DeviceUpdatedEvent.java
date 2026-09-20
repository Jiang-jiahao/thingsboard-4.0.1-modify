package com.jnks.iot.server.common.transport;

import lombok.Getter;
import com.jnks.iot.server.common.data.Device;

@Getter
public class DeviceUpdatedEvent {
    private final Device device;

    public DeviceUpdatedEvent(Device device) {
        this.device = device;
    }
}
