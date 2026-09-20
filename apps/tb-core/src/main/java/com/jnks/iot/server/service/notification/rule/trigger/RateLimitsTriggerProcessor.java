package com.jnks.iot.server.service.notification.rule.trigger;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.info.RateLimitsNotificationInfo;
import com.jnks.iot.server.common.data.notification.info.RuleOriginatedNotificationInfo;
import com.jnks.iot.server.common.data.notification.rule.trigger.RateLimitsTrigger;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.RateLimitsNotificationRuleTriggerConfig;
import com.jnks.iot.server.common.data.util.CollectionsUtil;
import com.jnks.iot.server.dao.entity.EntityService;
import com.jnks.iot.server.dao.tenant.TenantService;

import java.util.Optional;

@Service
@RequiredArgsConstructor
public class RateLimitsTriggerProcessor implements NotificationRuleTriggerProcessor<RateLimitsTrigger, RateLimitsNotificationRuleTriggerConfig> {

    private final TenantService tenantService;
    private final EntityService entityService;

    @Override
    public boolean matchesFilter(RateLimitsTrigger trigger, RateLimitsNotificationRuleTriggerConfig triggerConfig) {
        return trigger.getLimitLevel() != null && trigger.getApi().getLabel() != null &&
                CollectionsUtil.emptyOrContains(triggerConfig.getApis(), trigger.getApi());
    }

    @Override
    public RuleOriginatedNotificationInfo constructNotificationInfo(RateLimitsTrigger trigger) {
        EntityId limitLevel = trigger.getLimitLevel();
        String tenantName = tenantService.findTenantById(trigger.getTenantId()).getName();
        String limitLevelEntityName = null;
        if (limitLevel instanceof TenantId) {
            limitLevelEntityName = tenantName;
        } else if (limitLevel != null) {
            limitLevelEntityName = Optional.ofNullable(trigger.getLimitLevelEntityName())
                    .orElseGet(() -> entityService.fetchEntityName(trigger.getTenantId(), limitLevel).orElse(null));
        }
        return RateLimitsNotificationInfo.builder()
                .tenantId(trigger.getTenantId())
                .tenantName(tenantName)
                .api(trigger.getApi())
                .limitLevel(limitLevel)
                .limitLevelEntityName(limitLevelEntityName)
                .build();
    }

    @Override
    public NotificationRuleTriggerType getTriggerType() {
        return NotificationRuleTriggerType.RATE_LIMITS;
    }

}
