package com.jnks.iot.server.dao.service.validator;

import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.plugin.ComponentDescriptor;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.service.DataValidator;

@Component
public class ComponentDescriptorDataValidator extends DataValidator<ComponentDescriptor> {

    @Override
    protected void validateDataImpl(TenantId tenantId, ComponentDescriptor plugin) {
        validateString("Component name", plugin.getName());
        if (plugin.getType() == null) {
            throw new DataValidationException("Component type should be specified!");
        }
        if (plugin.getScope() == null) {
            throw new DataValidationException("Component scope should be specified!");
        }
        if (StringUtils.isEmpty(plugin.getClazz())) {
            throw new DataValidationException("Component clazz should be specified!");
        }
    }
}
