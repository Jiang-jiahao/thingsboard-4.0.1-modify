package com.jnks.iot.server.cache.device;

import lombok.Data;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;

@Data
public class DeviceCacheEvictEvent {

    private final TenantId tenantId;
    private final DeviceId deviceId;
    private final String newName;
    private final String oldName;
    private Device savedDevice;

}
