package com.jnks.iot.server.actors.calculatedField;

import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.JnksIotActor;
import com.jnks.iot.server.actors.JnksIotActorId;
import com.jnks.iot.server.actors.JnksIotCalculatedFieldEntityActorId;
import com.jnks.iot.server.actors.service.ContextBasedCreator;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;

public class CalculatedFieldEntityActorCreator extends ContextBasedCreator {

    private final TenantId tenantId;
    private final EntityId entityId;

    public CalculatedFieldEntityActorCreator(ActorSystemContext context, TenantId tenantId, EntityId entityId) {
        super(context);
        this.tenantId = tenantId;
        this.entityId = entityId;
    }

    @Override
    public JnksIotActorId createActorId() {
        return new JnksIotCalculatedFieldEntityActorId(entityId);
    }

    @Override
    public JnksIotActor createActor() {
        return new CalculatedFieldEntityActor(context, tenantId, entityId);
    }

}
