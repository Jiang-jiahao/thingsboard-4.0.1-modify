package com.jnks.iot.server.dao.eventsourcing;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;

@Builder
@Data
public class DeleteEntityEvent<T> {

    private final TenantId tenantId;
    private final EntityId entityId;
    private final T entity;
    private final String body;
    private final ActionCause cause;

    @Builder.Default
    private final long ts = System.currentTimeMillis();

}
