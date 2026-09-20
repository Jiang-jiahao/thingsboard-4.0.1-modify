package com.jnks.iot.server.actors.calculatedField;

import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.TbActor;
import com.jnks.iot.server.actors.TbActorId;
import com.jnks.iot.server.actors.TbCalculatedFieldEntityActorId;
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
    public TbActorId createActorId() {
        return new TbCalculatedFieldEntityActorId(entityId);
    }

    @Override
    public TbActor createActor() {
        return new CalculatedFieldEntityActor(context, tenantId, entityId);
    }

}
