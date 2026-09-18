package org.thingsboard.server.common.data.notification.rule.trigger;

import lombok.Builder;
import lombok.Data;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.limit.LimitedApi;
import org.thingsboard.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;

import java.util.concurrent.TimeUnit;

@Data
@Builder
public class RateLimitsTrigger implements NotificationRuleTrigger {

    private final TenantId tenantId;
    private final LimitedApi api;
    private final EntityId limitLevel;
    private final String limitLevelEntityName;

    @Override
    public NotificationRuleTriggerType getType() {
        return NotificationRuleTriggerType.RATE_LIMITS;
    }

    @Override
    public EntityId getOriginatorEntityId() {
        return limitLevel != null ? limitLevel : tenantId;
    }


    @Override
    public boolean deduplicate() {
        return true;
    }

    @Override
    public String getDeduplicationKey() {
        return String.join(":", NotificationRuleTrigger.super.getDeduplicationKey(), api.toString());
    }

    @Override
    public long getDefaultDeduplicationDuration() {
        return TimeUnit.HOURS.toMillis(4);
    }

}
