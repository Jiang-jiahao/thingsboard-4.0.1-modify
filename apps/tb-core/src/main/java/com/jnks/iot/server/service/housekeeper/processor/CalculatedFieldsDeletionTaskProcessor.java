package com.jnks.iot.server.service.housekeeper.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.housekeeper.HousekeeperTask;
import com.jnks.iot.server.common.data.housekeeper.HousekeeperTaskType;
import com.jnks.iot.server.dao.cf.CalculatedFieldService;

@Component
@RequiredArgsConstructor
@Slf4j
public class CalculatedFieldsDeletionTaskProcessor extends HousekeeperTaskProcessor<HousekeeperTask> {

    private final CalculatedFieldService calculatedFieldService;

    @Override
    public void process(HousekeeperTask task) throws Exception {
        int deletedCount = calculatedFieldService.deleteAllCalculatedFieldsByEntityId(task.getTenantId(), task.getEntityId());
        log.debug("[{}][{}][{}] Deleted {} calculated fields", task.getTenantId(), task.getEntityId().getEntityType(), task.getEntityId(), deletedCount);
    }

    @Override
    public HousekeeperTaskType getTaskType() {
        return HousekeeperTaskType.DELETE_CALCULATED_FIELDS;
    }

}
