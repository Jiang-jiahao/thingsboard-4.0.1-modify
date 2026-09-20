package com.jnks.iot.server.common.data.notification.rule.trigger;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;

@Data
@Builder
public class EntitiesLimitTrigger implements NotificationRuleTrigger {

    private final TenantId tenantId;
    private final EntityType entityType;

    private long limit;
    private long currentCount;

    @Override
    public NotificationRuleTriggerType getType() {
        return NotificationRuleTriggerType.ENTITIES_LIMIT;
    }

    @Override
    public EntityId getOriginatorEntityId() {
        return tenantId;
    }

}
