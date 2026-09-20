package com.jnks.iot.server.common.data.notification.rule.trigger;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.alarm.AlarmApiCallResult;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;

@Data
@Builder
public class AlarmTrigger implements NotificationRuleTrigger {

    private final TenantId tenantId;
    private final AlarmApiCallResult alarmUpdate;

    @Override
    public NotificationRuleTriggerType getType() {
        return NotificationRuleTriggerType.ALARM;
    }

    @Override
    public EntityId getOriginatorEntityId() {
        return alarmUpdate.getAlarm().getId();
    }

}
