package com.jnks.iot.server.service.notification.rule.trigger;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.UpdateMessage;
import com.jnks.iot.server.common.data.notification.info.NewPlatformVersionNotificationInfo;
import com.jnks.iot.server.common.data.notification.info.RuleOriginatedNotificationInfo;
import com.jnks.iot.server.common.data.notification.rule.trigger.NewPlatformVersionTrigger;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.NewPlatformVersionNotificationRuleTriggerConfig;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;

@Service
@RequiredArgsConstructor
public class NewPlatformVersionTriggerProcessor implements NotificationRuleTriggerProcessor<NewPlatformVersionTrigger, NewPlatformVersionNotificationRuleTriggerConfig> {

    @Override
    public boolean matchesFilter(NewPlatformVersionTrigger trigger, NewPlatformVersionNotificationRuleTriggerConfig triggerConfig) {
        return trigger.getUpdateInfo().isUpdateAvailable();
    }

    @Override
    public RuleOriginatedNotificationInfo constructNotificationInfo(NewPlatformVersionTrigger trigger) {
        UpdateMessage updateInfo = trigger.getUpdateInfo();
        return NewPlatformVersionNotificationInfo.builder()
                .latestVersion(updateInfo.getLatestVersion())
                .latestVersionReleaseNotesUrl(updateInfo.getLatestVersionReleaseNotesUrl())
                .upgradeInstructionsUrl(updateInfo.getUpgradeInstructionsUrl())
                .currentVersion(updateInfo.getCurrentVersion())
                .currentVersionReleaseNotesUrl(updateInfo.getCurrentVersionReleaseNotesUrl())
                .build();
    }

    @Override
    public NotificationRuleTriggerType getTriggerType() {
        return NotificationRuleTriggerType.NEW_PLATFORM_VERSION;
    }

}
