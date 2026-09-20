package com.jnks.iot.server.dao.service.validator;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.alarm.AlarmComment;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.service.DataValidator;

@Component
@AllArgsConstructor
public class AlarmCommentDataValidator extends DataValidator<AlarmComment> {

    @Override
    protected void validateDataImpl(TenantId tenantId, AlarmComment alarmComment) {
        if (alarmComment.getComment() == null) {
            throw new DataValidationException("Alarm comment should be specified!");
        }
        if (alarmComment.getAlarmId() == null) {
            throw new DataValidationException("Alarm id should be specified!");
        }
    }
}
