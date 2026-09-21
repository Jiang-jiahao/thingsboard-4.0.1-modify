package com.jnks.iot.server.service.housekeeper.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.housekeeper.AlarmsUnassignHousekeeperTask;
import com.jnks.iot.server.common.data.housekeeper.HousekeeperTaskType;
import com.jnks.iot.server.common.data.id.AlarmId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.dao.alarm.AlarmService;
import com.jnks.iot.server.service.entitiy.alarm.JnksIotAlarmService;

import java.util.List;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class AlarmsUnassignTaskProcessor extends HousekeeperTaskProcessor<AlarmsUnassignHousekeeperTask> {

    private final JnksIotAlarmService jnksIotAlarmService;
    private final AlarmService alarmService;

    @Override
    public void process(AlarmsUnassignHousekeeperTask task) throws Exception {
        TenantId tenantId = task.getTenantId();
        UserId userId = (UserId) task.getEntityId();
        if (task.getAlarms() == null) {
            AlarmId lastId = null;
            long lastCreatedTime = 0;
            while (true) {
                List<JnksIotPair<UUID, Long>> alarms = alarmService.findAlarmIdsByAssigneeId(tenantId, userId, lastCreatedTime, lastId, 64);
                if (alarms.isEmpty()) {
                    break;
                }
                housekeeperClient.submitTask(new AlarmsUnassignHousekeeperTask(tenantId, userId, task.getUserTitle(), alarms.stream().map(JnksIotPair::getFirst).toList()));

                JnksIotPair<UUID, Long> last = alarms.get(alarms.size() - 1);
                lastId = new AlarmId(last.getFirst());
                lastCreatedTime = last.getSecond();
                log.debug("[{}][{}] Submitted task for unassigning {} alarms", tenantId, userId, alarms.size());
            }
        } else {
            jnksIotAlarmService.unassignDeletedUserAlarms(tenantId, userId, task.getUserTitle(), task.getAlarms(), task.getTs());
            log.debug("[{}][{}] Unassigned {} alarms", tenantId, userId, task.getAlarms().size());
        }
    }

    @Override
    public HousekeeperTaskType getTaskType() {
        return HousekeeperTaskType.UNASSIGN_ALARMS;
    }

}
