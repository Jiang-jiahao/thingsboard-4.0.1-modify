package com.jnks.iot.server.common.data.notification.info;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.notification.NotificationLabels;
import com.jnks.iot.server.common.data.alarm.AlarmSeverity;
import com.jnks.iot.server.common.data.alarm.AlarmStatus;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.DashboardId;
import com.jnks.iot.server.common.data.id.EntityId;

import java.util.Map;
import java.util.UUID;

import static com.jnks.iot.server.common.data.util.CollectionsUtil.mapOf;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AlarmNotificationInfo implements RuleOriginatedNotificationInfo {

    private String alarmType;
    private String action;
    private UUID alarmId;
    private EntityId alarmOriginator;
    private String alarmOriginatorName;
    private AlarmSeverity alarmSeverity;
    private AlarmStatus alarmStatus;
    private boolean acknowledged;
    private boolean cleared;
    private CustomerId alarmCustomerId;
    private DashboardId dashboardId;

    @Override
    public Map<String, String> getTemplateData() {
        return mapOf(
                "alarmType", alarmType,
                "action", NotificationLabels.alarmAction(action),
                "alarmId", alarmId.toString(),
                "alarmSeverity", NotificationLabels.alarmSeverity(alarmSeverity),
                "alarmStatus", alarmStatus.toString(),
                "alarmOriginatorEntityType", NotificationLabels.entityType(alarmOriginator.getEntityType()),
                "alarmOriginatorName", alarmOriginatorName,
                "alarmOriginatorId", alarmOriginator.getId().toString()
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
