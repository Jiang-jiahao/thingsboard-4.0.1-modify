package com.jnks.iot.server.dao.service.validator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.ApiUsageState;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.service.DataValidator;
import com.jnks.iot.server.dao.tenant.TenantService;

@Component
public class ApiUsageDataValidator extends DataValidator<ApiUsageState> {

    @Lazy
    @Autowired
    private TenantService tenantService;

    @Override
    protected void validateDataImpl(TenantId requestTenantId, ApiUsageState apiUsageState) {
        if (apiUsageState.getTenantId() == null) {
            throw new DataValidationException("ApiUsageState should be assigned to tenant!");
        } else {
            if (!tenantService.tenantExists(apiUsageState.getTenantId()) && !requestTenantId.equals(TenantId.SYS_TENANT_ID)) {
                throw new DataValidationException("ApiUsageState is referencing to non-existent tenant!");
            }
        }
        if (apiUsageState.getEntityId() == null) {
            throw new DataValidationException("UsageRecord should be assigned to entity!");
        } else if (apiUsageState.getEntityId().getEntityType() != EntityType.TENANT && apiUsageState.getEntityId().getEntityType() != EntityType.CUSTOMER) {
            throw new DataValidationException("Only Tenant and Customer Usage Records are supported!");
        }
    }
}
