package com.jnks.iot.server.common.data.exception;

import lombok.Getter;
import com.jnks.iot.server.common.data.id.TenantId;

public class TenantNotFoundException extends RuntimeException {

    @Getter
    private final TenantId tenantId;

    public TenantNotFoundException(TenantId tenantId) {
        super("Tenant with id " + tenantId + " not found");
        this.tenantId = tenantId;
    }

}
