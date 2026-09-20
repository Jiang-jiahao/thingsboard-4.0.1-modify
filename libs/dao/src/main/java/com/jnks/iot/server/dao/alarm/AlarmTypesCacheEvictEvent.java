package com.jnks.iot.server.dao.alarm;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import com.jnks.iot.server.common.data.id.TenantId;

@Data
@RequiredArgsConstructor
class AlarmTypesCacheEvictEvent {
    private final TenantId tenantId;
}
