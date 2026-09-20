package com.jnks.iot.server.cache.device;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import com.jnks.iot.server.cache.VersionedCacheKey;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;

import java.io.Serial;

@Getter
@EqualsAndHashCode
@RequiredArgsConstructor
@Builder
public class DeviceCacheKey implements VersionedCacheKey {

    @Serial
    private static final long serialVersionUID = 6366389552842340207L;

    private final TenantId tenantId;
    private final DeviceId deviceId;
    private final String deviceName;

    public DeviceCacheKey(DeviceId deviceId) {
        this(null, deviceId, null);
    }

    public DeviceCacheKey(TenantId tenantId, DeviceId deviceId) {
        this(tenantId, deviceId, null);
    }

    public DeviceCacheKey(TenantId tenantId, String deviceName) {
        this(tenantId, null, deviceName);
    }

    @Override
    public String toString() {
        if (deviceId == null) {
            return tenantId + "_n_" + deviceName;
        } else if (tenantId == null) {
            return deviceId.toString();
        } else {
            return tenantId + "_" + deviceId;
        }
    }

    @Override
    public boolean isVersioned() {
        return deviceId != null;
    }

}
