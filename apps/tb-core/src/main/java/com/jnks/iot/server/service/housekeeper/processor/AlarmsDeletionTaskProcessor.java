package com.jnks.iot.server.service.housekeeper.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.housekeeper.AlarmsDeletionHousekeeperTask;
import com.jnks.iot.server.common.data.housekeeper.HousekeeperTaskType;
import com.jnks.iot.server.common.data.id.AlarmId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.util.TbPair;
import com.jnks.iot.server.dao.alarm.AlarmService;

import java.util.List;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class AlarmsDeletionTaskProcessor extends HousekeeperTaskProcessor<AlarmsDeletionHousekeeperTask> {

    private final AlarmService alarmService;

    @Override
    public void process(AlarmsDeletionHousekeeperTask task) throws Exception {
        EntityId entityId = task.getEntityId();
        EntityType entityType = entityId.getEntityType();
        TenantId tenantId = task.getTenantId();

        if (entityType == EntityType.DEVICE || entityType == EntityType.ASSET) {
            if (task.getAlarms() == null) {
                AlarmId lastId = null;
                long lastCreatedTime = 0;
                while (true) {
                    List<TbPair<UUID, Long>> alarms = alarmService.findAlarmIdsByOriginatorId(tenantId, entityId, lastCreatedTime, lastId, 128);
                    if (alarms.isEmpty()) {
                        break;
                    }

                    housekeeperClient.submitTask(new AlarmsDeletionHousekeeperTask(tenantId, entityId, alarms.stream().map(TbPair::getFirst).toList()));

                    TbPair<UUID, Long> last = alarms.get(alarms.size() - 1);
                    lastId = new AlarmId(last.getFirst());
                    lastCreatedTime = last.getSecond();
                    log.debug("[{}][{}][{}] Submitted task for deleting {} alarms", tenantId, entityType, entityId, alarms.size());
                }
            } else {
                for (UUID alarmId : task.getAlarms()) {
                    alarmService.delAlarm(tenantId, new AlarmId(alarmId));
                }
                log.debug("[{}][{}][{}] Deleted {} alarms", tenantId, entityType, entityId, task.getAlarms().size());
            }
        }

        int count = alarmService.deleteEntityAlarmRecords(tenantId, entityId);
        log.debug("[{}][{}][{}] Deleted {} entity alarms", tenantId, entityType, entityId, count);
    }

    @Override
    public HousekeeperTaskType getTaskType() {
        return HousekeeperTaskType.DELETE_ALARMS;
    }

}
