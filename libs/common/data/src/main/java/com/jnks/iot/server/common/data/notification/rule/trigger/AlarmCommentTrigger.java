package com.jnks.iot.server.common.data.notification.rule.trigger;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.alarm.Alarm;
import com.jnks.iot.server.common.data.alarm.AlarmComment;
import com.jnks.iot.server.common.data.audit.ActionType;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;

@Data
@Builder
public class AlarmCommentTrigger implements NotificationRuleTrigger {

    private final TenantId tenantId;
    private final AlarmComment comment;
    private final Alarm alarm;
    private final ActionType actionType;
    private final User user;

    @Override
    public NotificationRuleTriggerType getType() {
        return NotificationRuleTriggerType.ALARM_COMMENT;
    }

    @Override
    public EntityId getOriginatorEntityId() {
        return alarm.getId();
    }

}
