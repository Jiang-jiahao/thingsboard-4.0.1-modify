package com.jnks.iot.server.dao.entity;

import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.TenantId;

public interface EntityCountService {

    long countByTenantIdAndEntityType(TenantId tenantId, EntityType entityType);

    void publishCountEntityEvictEvent(TenantId tenantId, EntityType entityType);
}
