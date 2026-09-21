package com.jnks.iot.server.service.housekeeper.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.housekeeper.HousekeeperTaskType;
import com.jnks.iot.server.common.data.housekeeper.TsHistoryDeletionHousekeeperTask;
import com.jnks.iot.server.common.data.kv.BaseDeleteTsKvQuery;
import com.jnks.iot.server.common.data.kv.DeleteTsKvQuery;
import com.jnks.iot.server.dao.timeseries.TimeseriesService;

import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class TsHistoryDeletionTaskProcessor extends HousekeeperTaskProcessor<TsHistoryDeletionHousekeeperTask> {

    private final TimeseriesService timeseriesService;

    @Override
    public void process(TsHistoryDeletionHousekeeperTask task) throws Exception {
        DeleteTsKvQuery deleteQuery = new BaseDeleteTsKvQuery(task.getKey(), 0, System.currentTimeMillis(), false, false);
        wait(timeseriesService.remove(task.getTenantId(), task.getEntityId(), List.of(deleteQuery)));
        log.debug("[{}][{}][{}] Deleted timeseries history for key '{}'", task.getTenantId(), task.getEntityId().getEntityType(), task.getEntityId(), task.getKey());
    }

    @Override
    public HousekeeperTaskType getTaskType() {
        return HousekeeperTaskType.DELETE_TS_HISTORY;
    }

}
