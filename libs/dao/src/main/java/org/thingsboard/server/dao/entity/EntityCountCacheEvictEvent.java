package org.thingsboard.server.dao.entity;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.id.TenantId;

@Data
@RequiredArgsConstructor
class EntityCountCacheEvictEvent {
    private final TenantId tenantId;
    private final EntityType entityType;
}
