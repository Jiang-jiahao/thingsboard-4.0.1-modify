package com.jnks.iot.server.actors.calculatedField;

import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.TbActor;
import com.jnks.iot.server.actors.TbActorId;
import com.jnks.iot.server.actors.TbStringActorId;
import com.jnks.iot.server.actors.service.ContextBasedCreator;
import com.jnks.iot.server.common.data.id.TenantId;

public class CalculatedFieldManagerActorCreator extends ContextBasedCreator {

    private final TenantId tenantId;

    public CalculatedFieldManagerActorCreator(ActorSystemContext context, TenantId tenantId) {
        super(context);
        this.tenantId = tenantId;
    }

    @Override
    public TbActorId createActorId() {
        return new TbStringActorId("CFM|" + tenantId);
    }

    @Override
    public TbActor createActor() {
        return new CalculatedFieldManagerActor(context, tenantId);
    }

}
