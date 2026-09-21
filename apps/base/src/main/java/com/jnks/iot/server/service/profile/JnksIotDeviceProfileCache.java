package com.jnks.iot.server.service.profile;

import com.jnks.iot.rule.engine.api.RuleEngineDeviceProfileCache;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.common.data.id.TenantId;

public interface JnksIotDeviceProfileCache extends RuleEngineDeviceProfileCache {

    void evict(TenantId tenantId, DeviceProfileId id);

    void evict(TenantId tenantId, DeviceId id);

    DeviceProfile find(DeviceProfileId deviceProfileId);

    DeviceProfile findOrCreateDeviceProfile(TenantId tenantId, String deviceType);
}
