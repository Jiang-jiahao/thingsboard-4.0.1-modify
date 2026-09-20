package com.jnks.iot.server.common.data.notification.rule.trigger;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.ApiUsageRecordState;
import com.jnks.iot.server.common.data.ApiUsageStateValue;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;

@Data
@Builder
public class ApiUsageLimitTrigger implements NotificationRuleTrigger {

    private final TenantId tenantId;
    private final ApiUsageRecordState state;
    private final ApiUsageStateValue status;

    @Override
    public NotificationRuleTriggerType getType() {
        return NotificationRuleTriggerType.API_USAGE_LIMIT;
    }

    @Override
    public EntityId getOriginatorEntityId() {
        return tenantId;
    }

}
