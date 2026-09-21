package com.jnks.iot.server.common.transport;

import lombok.Getter;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.queue.discovery.event.JnksIotApplicationEvent;

public final class DeviceProfileUpdatedEvent extends JnksIotApplicationEvent {

    @Getter
    private final DeviceProfile deviceProfile;

    public DeviceProfileUpdatedEvent(DeviceProfile deviceProfile) {
        super(new Object());
        this.deviceProfile = deviceProfile;
    }
}
