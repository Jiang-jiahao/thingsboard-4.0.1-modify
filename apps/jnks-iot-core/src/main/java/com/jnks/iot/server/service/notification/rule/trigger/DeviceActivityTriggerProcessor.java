package com.jnks.iot.server.service.notification.rule.trigger;

import lombok.RequiredArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.info.DeviceActivityNotificationInfo;
import com.jnks.iot.server.common.data.notification.info.RuleOriginatedNotificationInfo;
import com.jnks.iot.server.common.data.notification.rule.trigger.DeviceActivityTrigger;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.DeviceActivityNotificationRuleTriggerConfig;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.DeviceActivityNotificationRuleTriggerConfig.DeviceEvent;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;
import com.jnks.iot.server.service.profile.JnksIotDeviceProfileCache;

@Service
@RequiredArgsConstructor
public class DeviceActivityTriggerProcessor implements NotificationRuleTriggerProcessor<DeviceActivityTrigger, DeviceActivityNotificationRuleTriggerConfig> {

    private final JnksIotDeviceProfileCache deviceProfileCache;

    @Override
    public boolean matchesFilter(DeviceActivityTrigger trigger, DeviceActivityNotificationRuleTriggerConfig triggerConfig) {
        DeviceEvent event = trigger.isActive() ? DeviceEvent.ACTIVE : DeviceEvent.INACTIVE;
        if (!triggerConfig.getNotifyOn().contains(event)) {
            return false;
        }
        DeviceId deviceId = trigger.getDeviceId();
        if (CollectionUtils.isNotEmpty(triggerConfig.getDevices())) {
            return triggerConfig.getDevices().contains(deviceId.getId());
        } else if (CollectionUtils.isNotEmpty(triggerConfig.getDeviceProfiles())) {
            DeviceProfile deviceProfile = deviceProfileCache.get(TenantId.SYS_TENANT_ID, deviceId);
            return deviceProfile != null && triggerConfig.getDeviceProfiles().contains(deviceProfile.getUuidId());
        } else {
            return true;
        }
    }

    @Override
    public RuleOriginatedNotificationInfo constructNotificationInfo(DeviceActivityTrigger trigger) {
        return DeviceActivityNotificationInfo.builder()
                .eventType(trigger.isActive() ? "active" : "inactive")
                .deviceId(trigger.getDeviceId().getId())
                .deviceName(trigger.getDeviceName())
                .deviceType(trigger.getDeviceType())
                .deviceLabel(trigger.getDeviceLabel())
                .deviceCustomerId(trigger.getCustomerId())
                .build();
    }

    @Override
    public NotificationRuleTriggerType getTriggerType() {
        return NotificationRuleTriggerType.DEVICE_ACTIVITY;
    }

}
