package org.thingsboard.server.dao.service.validator;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;
import org.thingsboard.server.common.data.alarm.AlarmComment;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.dao.exception.DataValidationException;
import org.thingsboard.server.dao.service.DataValidator;

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
