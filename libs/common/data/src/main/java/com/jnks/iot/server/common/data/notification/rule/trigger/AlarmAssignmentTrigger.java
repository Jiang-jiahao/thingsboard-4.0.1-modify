package com.jnks.iot.server.common.data.notification.rule.trigger;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.alarm.AlarmInfo;
import com.jnks.iot.server.common.data.audit.ActionType;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;

@Data
@Builder
public class AlarmAssignmentTrigger implements NotificationRuleTrigger {

    private final TenantId tenantId;
    private final AlarmInfo alarmInfo;
    private final ActionType actionType;
    private final User user;

    @Override
    public EntityId getOriginatorEntityId() {
        return alarmInfo.getOriginator();
    }

    @Override
    public NotificationRuleTriggerType getType() {
        return NotificationRuleTriggerType.ALARM_ASSIGNMENT;
    }

}
