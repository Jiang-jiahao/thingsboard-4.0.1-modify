package org.thingsboard.server.service.housekeeper.processor;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.thingsboard.server.common.data.housekeeper.HousekeeperTask;
import org.thingsboard.server.common.data.housekeeper.HousekeeperTaskType;
import org.thingsboard.server.dao.event.EventService;

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
