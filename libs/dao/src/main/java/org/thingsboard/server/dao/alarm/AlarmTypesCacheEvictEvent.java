package org.thingsboard.server.dao.alarm;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.thingsboard.server.common.data.id.TenantId;

@Data
@RequiredArgsConstructor
class AlarmTypesCacheEvictEvent {
    private final TenantId tenantId;
}
