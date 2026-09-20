package com.jnks.iot.server.dao.event;

import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.server.common.data.EventInfo;
import com.jnks.iot.server.common.data.event.Event;
import com.jnks.iot.server.common.data.event.EventFilter;
import com.jnks.iot.server.common.data.event.EventType;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.TimePageLink;

import java.util.List;

public interface EventService {

    ListenableFuture<Void> saveAsync(Event event);

    PageData<EventInfo> findEvents(TenantId tenantId, EntityId entityId, EventType eventType, TimePageLink pageLink);

    List<EventInfo> findLatestEvents(TenantId tenantId, EntityId entityId, EventType eventType, int limit);

    EventInfo findLatestDebugRuleNodeInEvent(TenantId tenantId, EntityId entityId);

    PageData<EventInfo> findEventsByFilter(TenantId tenantId, EntityId entityId, EventFilter eventFilter, TimePageLink pageLink);

    void removeEvents(TenantId tenantId, EntityId entityId);

    void removeEvents(TenantId tenantId, EntityId entityId, EventFilter eventFilter, Long startTime, Long endTime);

    void cleanupEvents(long regularEventExpTs, long debugEventExpTs, boolean cleanupDb);

}
