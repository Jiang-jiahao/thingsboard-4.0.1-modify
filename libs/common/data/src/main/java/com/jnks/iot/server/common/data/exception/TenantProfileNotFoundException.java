package com.jnks.iot.server.common.data.exception;

import lombok.Getter;
import com.jnks.iot.server.common.data.id.TenantId;

public class TenantProfileNotFoundException extends RuntimeException {

    @Getter
    private final TenantId tenantId;

    public TenantProfileNotFoundException(TenantId tenantId) {
        super("Profile for tenant with id " + tenantId + " not found");
        this.tenantId = tenantId;
    }

}
