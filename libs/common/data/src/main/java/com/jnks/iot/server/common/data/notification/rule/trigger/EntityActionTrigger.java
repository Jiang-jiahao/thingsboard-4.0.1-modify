package com.jnks.iot.server.common.data.notification.rule.trigger;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.HasName;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.audit.ActionType;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;

@Data
@Builder
public class EntityActionTrigger implements NotificationRuleTrigger {

    private final TenantId tenantId;
    private final EntityId entityId;
    private final HasName entity;
    private final ActionType actionType;
    private final User user;

    @Override
    public NotificationRuleTriggerType getType() {
        return NotificationRuleTriggerType.ENTITY_ACTION;
    }

    @Override
    public EntityId getOriginatorEntityId() {
        return entityId;
    }

}
