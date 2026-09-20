package com.jnks.iot.server.dao.device;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.common.data.id.TenantId;

@Data
@RequiredArgsConstructor
@AllArgsConstructor
public class DeviceProfileEvictEvent {

    private final TenantId tenantId;
    private final String newName;
    private final String oldName;
    private final DeviceProfileId deviceProfileId;
    private final boolean defaultProfile;
    private final String provisionDeviceKey;
    private DeviceProfile savedDeviceProfile;

}
