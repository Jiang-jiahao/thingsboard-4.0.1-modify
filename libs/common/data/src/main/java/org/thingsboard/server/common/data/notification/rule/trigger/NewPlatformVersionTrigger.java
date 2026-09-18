package org.thingsboard.server.common.data.notification.rule.trigger;

import lombok.Builder;
import lombok.Data;
import org.thingsboard.server.common.data.UpdateMessage;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;

@Data
@Builder
public class NewPlatformVersionTrigger implements NotificationRuleTrigger {

    private final UpdateMessage updateInfo;

    @Override
    public NotificationRuleTriggerType getType() {
        return NotificationRuleTriggerType.NEW_PLATFORM_VERSION;
    }

    @Override
    public TenantId getTenantId() {
        return TenantId.SYS_TENANT_ID;
    }

    @Override
    public EntityId getOriginatorEntityId() {
        return TenantId.SYS_TENANT_ID;
    }


    @Override
    public boolean deduplicate() {
        return true;
    }

    @Override
    public String getDeduplicationKey() {
        return String.join(":", NotificationRuleTrigger.super.getDeduplicationKey(),
                updateInfo.getCurrentVersion(), updateInfo.getLatestVersion());
    }

    @Override
    public long getDefaultDeduplicationDuration() {
        return 0;
    }

}
