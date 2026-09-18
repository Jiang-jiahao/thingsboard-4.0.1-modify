package org.thingsboard.server.common.data.notification.rule.trigger;

import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;

import java.io.Serializable;

public interface NotificationRuleTrigger extends Serializable {

    NotificationRuleTriggerType getType();

    TenantId getTenantId();

    EntityId getOriginatorEntityId();


    default boolean deduplicate() {
        return false;
    }

    default String getDeduplicationKey() {
        EntityId originatorEntityId = getOriginatorEntityId();
        return String.join(":", getType().toString(), originatorEntityId.getEntityType().toString(), originatorEntityId.getId().toString());
    }

    default long getDefaultDeduplicationDuration() {
        return 0;
    }

}
