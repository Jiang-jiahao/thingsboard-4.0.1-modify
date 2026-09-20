package com.jnks.iot.server.dao.service.validator;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.alarm.Alarm;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.service.DataValidator;
import com.jnks.iot.server.dao.tenant.TenantService;

@Component
@AllArgsConstructor
public class AlarmDataValidator extends DataValidator<Alarm> {

    private final TenantService tenantService;

    @Override
    protected void validateDataImpl(TenantId tenantId, Alarm alarm) {
        validateString("Alarm type", alarm.getType());
        if (alarm.getOriginator() == null) {
            throw new DataValidationException("Alarm originator should be specified!");
        }
        if (alarm.getSeverity() == null) {
            throw new DataValidationException("Alarm severity should be specified!");
        }
        if (alarm.getStatus() == null) {
            throw new DataValidationException("Alarm status should be specified!");
        }
        if (alarm.getTenantId() == null) {
            throw new DataValidationException("Alarm should be assigned to tenant!");
        } else {
            if (!tenantService.tenantExists(alarm.getTenantId())) {
                throw new DataValidationException("Alarm is referencing to non-existent tenant!");
            }
        }
    }
}
