package com.jnks.iot.server.dao.service.validator;

import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.oauth2.OAuth2ClientRegistrationTemplate;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.service.DataValidator;

@Component
public class ClientRegistrationTemplateDataValidator extends DataValidator<OAuth2ClientRegistrationTemplate> {

    @Override
    protected void validateCreate(TenantId tenantId, OAuth2ClientRegistrationTemplate clientRegistrationTemplate) {
    }

    @Override
    protected OAuth2ClientRegistrationTemplate validateUpdate(TenantId tenantId, OAuth2ClientRegistrationTemplate clientRegistrationTemplate) {
        return null;
    }

    @Override
    protected void validateDataImpl(TenantId tenantId, OAuth2ClientRegistrationTemplate clientRegistrationTemplate) {
        if (StringUtils.isEmpty(clientRegistrationTemplate.getProviderId())) {
            throw new DataValidationException("Provider ID should be specified!");
        }
        if (clientRegistrationTemplate.getMapperConfig() == null) {
            throw new DataValidationException("Mapper config should be specified!");
        }
        if (clientRegistrationTemplate.getMapperConfig().getType() == null) {
            throw new DataValidationException("Mapper type should be specified!");
        }
        if (clientRegistrationTemplate.getMapperConfig().getBasic() == null) {
            throw new DataValidationException("Basic mapper config should be specified!");
        }
    }
}
