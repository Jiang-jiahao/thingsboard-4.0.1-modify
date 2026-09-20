package com.jnks.iot.server.common.data.notification.info;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.alarm.AlarmSeverity;
import com.jnks.iot.server.common.data.alarm.AlarmStatus;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.DashboardId;
import com.jnks.iot.server.common.data.id.EntityId;

import java.util.Map;
import java.util.UUID;

import static com.jnks.iot.server.common.data.util.CollectionsUtil.mapOf;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class AlarmCommentNotificationInfo implements RuleOriginatedNotificationInfo {

    private String comment;
    private String action;

    private String userEmail;
    private String userFirstName;
    private String userLastName;

    private String alarmType;
    private UUID alarmId;
    private EntityId alarmOriginator;
    private String alarmOriginatorName;
    private AlarmSeverity alarmSeverity;
    private AlarmStatus alarmStatus;
    private CustomerId alarmCustomerId;
    private DashboardId dashboardId;

    @Override
    public Map<String, String> getTemplateData() {
        return mapOf(
                "comment", comment,
                "action", action,
                "userTitle", User.getTitle(userEmail, userFirstName, userLastName),
                "userEmail", userEmail,
                "userFirstName", userFirstName,
                "userLastName", userLastName,
                "alarmType", alarmType,
                "alarmId", alarmId.toString(),
                "alarmSeverity", alarmSeverity.name().toLowerCase(),
                "alarmStatus", alarmStatus.toString(),
                "alarmOriginatorEntityType", alarmOriginator.getEntityType().getNormalName(),
                "alarmOriginatorId", alarmOriginator.getId().toString(),
                "alarmOriginatorName", alarmOriginatorName
        );
    }

    @Override
    public CustomerId getAffectedCustomerId() {
        return alarmCustomerId;
    }

    @Override
    public EntityId getStateEntityId() {
        return alarmOriginator;
    }

    @Override
    public DashboardId getDashboardId() {
        return dashboardId;
    }

}
