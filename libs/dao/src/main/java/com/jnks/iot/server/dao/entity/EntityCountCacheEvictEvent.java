package com.jnks.iot.server.dao.entity;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.TenantId;

@Data
@RequiredArgsConstructor
class EntityCountCacheEvictEvent {
    private final TenantId tenantId;
    private final EntityType entityType;
}
