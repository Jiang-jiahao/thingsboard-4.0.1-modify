package com.jnks.iot.server.common.transport;

import lombok.Getter;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.queue.discovery.event.JnksIotApplicationEvent;

/**
 * 设备档案被删除：传输侧需要据此释放只由该档案占用的资源（如 TCP/UDP 自定义监听端口）。
 */
public final class DeviceProfileDeletedEvent extends JnksIotApplicationEvent {

    private static final long serialVersionUID = -2434875478955609345L;
    @Getter
    private final DeviceProfileId deviceProfileId;

    public DeviceProfileDeletedEvent(DeviceProfileId deviceProfileId) {
        super(new Object());
        this.deviceProfileId = deviceProfileId;
    }
}
