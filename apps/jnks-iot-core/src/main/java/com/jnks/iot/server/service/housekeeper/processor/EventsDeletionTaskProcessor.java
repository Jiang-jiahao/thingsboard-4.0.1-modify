package com.jnks.iot.server.service.housekeeper.processor;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.housekeeper.HousekeeperTask;
import com.jnks.iot.server.common.data.housekeeper.HousekeeperTaskType;
import com.jnks.iot.server.dao.event.EventService;

@Component
@RequiredArgsConstructor
public class EventsDeletionTaskProcessor extends HousekeeperTaskProcessor<HousekeeperTask> {

    private final EventService eventService;

    @Override
    public void process(HousekeeperTask task) throws Exception {
        eventService.removeEvents(task.getTenantId(), task.getEntityId(), null, 0L, System.currentTimeMillis());
    }

    @Override
    public HousekeeperTaskType getTaskType() {
        return HousekeeperTaskType.DELETE_EVENTS;
    }

}
