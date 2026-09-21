package com.jnks.iot.server.service.housekeeper.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.housekeeper.HousekeeperTaskType;
import com.jnks.iot.server.common.data.housekeeper.LatestTsDeletionHousekeeperTask;
import com.jnks.iot.server.dao.timeseries.TimeseriesService;

import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class LatestTsDeletionTaskProcessor extends HousekeeperTaskProcessor<LatestTsDeletionHousekeeperTask> {

    private final TimeseriesService timeseriesService;

    @Override
    public void process(LatestTsDeletionHousekeeperTask task) throws Exception {
        wait(timeseriesService.removeLatest(task.getTenantId(), task.getEntityId(), List.of(task.getKey())));
        log.debug("[{}][{}][{}] Deleted latest telemetry for key '{}'", task.getTenantId(), task.getEntityId().getEntityType(), task.getEntityId(), task.getKey());
    }

    @Override
    public HousekeeperTaskType getTaskType() {
        return HousekeeperTaskType.DELETE_LATEST_TS;
    }

}
