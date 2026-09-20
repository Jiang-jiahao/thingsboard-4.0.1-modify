package com.jnks.iot.server.dao.service.validator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.Tenant;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.service.DataValidator;
import com.jnks.iot.server.dao.tenant.TenantDao;

@Component
public class TenantDataValidator extends DataValidator<Tenant> {

    @Autowired
    private TenantDao tenantDao;

    @Override
    protected void validateDataImpl(TenantId tenantId, Tenant tenant) {
        validateString("Tenant title", tenant.getTitle());
        if (!StringUtils.isEmpty(tenant.getEmail())) {
            validateEmail(tenant.getEmail());
        }
    }

    @Override
    protected Tenant validateUpdate(TenantId tenantId, Tenant tenant) {
        Tenant old = tenantDao.findById(TenantId.SYS_TENANT_ID, tenantId.getId());
        if (old == null) {
            throw new DataValidationException("Can't update non existing tenant!");
        }
        return old;
    }

}
