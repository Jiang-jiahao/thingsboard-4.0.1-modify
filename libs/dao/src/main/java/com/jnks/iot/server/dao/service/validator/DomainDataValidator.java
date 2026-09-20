package com.jnks.iot.server.dao.service.validator;

import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.domain.Domain;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.exception.IncorrectParameterException;

@Component
public class DomainDataValidator extends AbstractHasOtaPackageValidator<Domain> {

    @Override
    protected void validateDataImpl(TenantId tenantId, Domain domain) {
        if (!isValidDomain(domain.getName())) {
            throw new IncorrectParameterException("Domain name " + domain.getName() + " is invalid");
        }
    }
}
