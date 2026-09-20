package com.jnks.iot.server.dao.exception;

import lombok.Getter;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.TenantId;

public class EntitiesLimitException extends DataValidationException {
    private static final long serialVersionUID = -9211462514373279196L;

    @Getter
    private final TenantId tenantId;
    @Getter
    private final EntityType entityType;

    public EntitiesLimitException(TenantId tenantId, EntityType entityType) {
        super(entityType.getNormalName() + "s limit reached");
        this.tenantId = tenantId;
        this.entityType = entityType;
    }
}
