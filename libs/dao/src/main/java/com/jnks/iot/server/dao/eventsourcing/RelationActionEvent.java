package com.jnks.iot.server.dao.eventsourcing;

import lombok.Data;
import com.jnks.iot.server.common.data.audit.ActionType;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.relation.EntityRelation;

@Data
public class RelationActionEvent {
    private final TenantId tenantId;
    private final EntityRelation relation;
    private final ActionType actionType;
}
