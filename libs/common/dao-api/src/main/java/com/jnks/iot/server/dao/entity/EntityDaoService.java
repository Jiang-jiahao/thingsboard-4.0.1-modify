package com.jnks.iot.server.dao.entity;

import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.HasId;
import com.jnks.iot.server.common.data.id.TenantId;

import java.util.Optional;

public interface EntityDaoService {

    Optional<HasId<?>> findEntity(TenantId tenantId, EntityId entityId);

    default long countByTenantId(TenantId tenantId) {
        throw new IllegalArgumentException("Not implemented for " + getEntityType());
    }

    default void deleteEntity(TenantId tenantId, EntityId id, boolean force) {
        throw new IllegalArgumentException(getEntityType().getNormalName() + " deletion not supported");
    }

    default void deleteByTenantId(TenantId tenantId) {
        throw new IllegalArgumentException("Deletion by tenant id not supported for " + getEntityType().getNormalName());
    }

    EntityType getEntityType();

}
