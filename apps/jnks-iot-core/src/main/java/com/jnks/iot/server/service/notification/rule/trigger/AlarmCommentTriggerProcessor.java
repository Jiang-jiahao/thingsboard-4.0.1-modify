package com.jnks.iot.server.service.notification.rule.trigger;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.alarm.Alarm;
import com.jnks.iot.server.common.data.alarm.AlarmCommentType;
import com.jnks.iot.server.common.data.alarm.AlarmInfo;
import com.jnks.iot.server.common.data.alarm.AlarmStatusFilter;
import com.jnks.iot.server.common.data.audit.ActionType;
import com.jnks.iot.server.common.data.notification.info.AlarmCommentNotificationInfo;
import com.jnks.iot.server.common.data.notification.info.RuleOriginatedNotificationInfo;
import com.jnks.iot.server.common.data.notification.rule.trigger.AlarmCommentTrigger;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.AlarmCommentNotificationRuleTriggerConfig;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;
import com.jnks.iot.server.dao.entity.EntityService;

import static org.apache.commons.collections4.CollectionUtils.isEmpty;
import static com.jnks.iot.server.common.data.util.CollectionsUtil.emptyOrContains;

@Service
@RequiredArgsConstructor
public class AlarmCommentTriggerProcessor implements NotificationRuleTriggerProcessor<AlarmCommentTrigger, AlarmCommentNotificationRuleTriggerConfig> {

    private final EntityService entityService;

    @Override
    public boolean matchesFilter(AlarmCommentTrigger trigger, AlarmCommentNotificationRuleTriggerConfig triggerConfig) {
        if (trigger.getActionType() == ActionType.UPDATED_COMMENT && !triggerConfig.isNotifyOnCommentUpdate()) {
            return false;
        }
        if (triggerConfig.isOnlyUserComments()) {
            if (trigger.getComment().getType() == AlarmCommentType.SYSTEM) {
                return false;
            }
        }
        Alarm alarm = trigger.getAlarm();
        return emptyOrContains(triggerConfig.getAlarmTypes(), alarm.getType()) &&
                emptyOrContains(triggerConfig.getAlarmSeverities(), alarm.getSeverity()) &&
                (isEmpty(triggerConfig.getAlarmStatuses()) || AlarmStatusFilter.from(triggerConfig.getAlarmStatuses()).matches(alarm));
    }

    @Override
    public RuleOriginatedNotificationInfo constructNotificationInfo(AlarmCommentTrigger trigger) {
        Alarm alarm = trigger.getAlarm();
        String originatorName;
        if (alarm instanceof AlarmInfo) {
            originatorName = ((AlarmInfo) alarm).getOriginatorName();
        } else {
            originatorName = entityService.fetchEntityName(trigger.getTenantId(), alarm.getOriginator()).orElse("");
        }
        return AlarmCommentNotificationInfo.builder()
                .comment(trigger.getComment().getComment().get("text").asText())
                .action(trigger.getActionType() == ActionType.ADDED_COMMENT ? "added" : "updated")
                .userEmail(trigger.getUser().getEmail())
                .userFirstName(trigger.getUser().getFirstName())
                .userLastName(trigger.getUser().getLastName())
                .alarmId(alarm.getUuidId())
                .alarmType(alarm.getType())
                .alarmOriginator(alarm.getOriginator())
                .alarmOriginatorName(originatorName)
                .alarmSeverity(alarm.getSeverity())
                .alarmStatus(alarm.getStatus())
                .alarmCustomerId(alarm.getCustomerId())
                .dashboardId(alarm.getDashboardId())
                .build();
    }

    @Override
    public NotificationRuleTriggerType getTriggerType() {
        return NotificationRuleTriggerType.ALARM_COMMENT;
    }

}
