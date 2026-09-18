package org.thingsboard.server.common.data.notification.rule.trigger;

import lombok.Builder;
import lombok.Data;
import org.thingsboard.server.common.data.housekeeper.HousekeeperTask;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;

@Data
@Builder
public class TaskProcessingFailureTrigger implements NotificationRuleTrigger {

    private final HousekeeperTask task;
    private final int attempt;
    private final Throwable error;

    @Override
    public NotificationRuleTriggerType getType() {
        return NotificationRuleTriggerType.TASK_PROCESSING_FAILURE;
    }

    @Override
    public TenantId getTenantId() {
        return task.getTenantId();
    }

    @Override
    public EntityId getOriginatorEntityId() {
        return task.getEntityId();
    }

    @Override
    public boolean deduplicate() {
        return false;
    }

}
