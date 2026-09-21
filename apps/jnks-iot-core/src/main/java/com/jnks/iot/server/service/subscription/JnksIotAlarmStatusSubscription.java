package com.jnks.iot.server.service.subscription;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import com.jnks.iot.server.common.data.alarm.AlarmInfo;
import com.jnks.iot.server.common.data.alarm.AlarmSeverity;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.service.ws.telemetry.sub.AlarmSubscriptionUpdate;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;


public class JnksIotAlarmStatusSubscription extends JnksIotSubscription<AlarmSubscriptionUpdate> {

    @Getter
    private final Set<UUID> alarmIds = new HashSet<>();
    @Getter
    @Setter
    private boolean hasMoreAlarmsInDB;
    @Getter
    private final List<String> typeList;
    @Getter
    private final List<AlarmSeverity> severityList;

    @Builder
    public JnksIotAlarmStatusSubscription(String serviceId, String sessionId, int subscriptionId, TenantId tenantId, EntityId entityId,
                                     BiConsumer<JnksIotSubscription<AlarmSubscriptionUpdate>, AlarmSubscriptionUpdate> updateProcessor,
                                     List<String> typeList, List<AlarmSeverity> severityList) {
        super(serviceId, sessionId, subscriptionId, tenantId, entityId, JnksIotSubscriptionType.ALARMS, updateProcessor);
        this.typeList = typeList;
        this.severityList = severityList;
    }

    public boolean matches(AlarmInfo alarm) {
        return !alarm.isCleared() && (this.typeList == null || this.typeList.contains(alarm.getType())) &&
                (this.severityList == null || this.severityList.contains(alarm.getSeverity()));
    }

    public boolean hasAlarms() {
        return !alarmIds.isEmpty();
    }
}
