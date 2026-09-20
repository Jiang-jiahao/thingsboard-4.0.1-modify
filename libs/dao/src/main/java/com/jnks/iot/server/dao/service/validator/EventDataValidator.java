package com.jnks.iot.server.dao.service.validator;

import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.event.Event;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.service.DataValidator;

@Component
public class EventDataValidator extends DataValidator<Event> {

    @Override
    protected void validateDataImpl(TenantId tenantId, Event event) {
        if (event.getTenantId() == null) {
            throw new DataValidationException("Tenant id should be specified!.");
        }
        if (event.getEntityId() == null) {
            throw new DataValidationException("Entity id should be specified!.");
        }
        if (StringUtils.isEmpty(event.getServiceId())) {
            throw new DataValidationException("Service id should be specified!.");
        }
    }
}
