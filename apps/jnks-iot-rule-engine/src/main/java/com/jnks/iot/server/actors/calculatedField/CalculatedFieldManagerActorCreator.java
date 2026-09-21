package com.jnks.iot.server.actors.calculatedField;

import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.JnksIotActor;
import com.jnks.iot.server.actors.JnksIotActorId;
import com.jnks.iot.server.actors.JnksIotStringActorId;
import com.jnks.iot.server.actors.service.ContextBasedCreator;
import com.jnks.iot.server.common.data.id.TenantId;

public class CalculatedFieldManagerActorCreator extends ContextBasedCreator {

    private final TenantId tenantId;

    public CalculatedFieldManagerActorCreator(ActorSystemContext context, TenantId tenantId) {
        super(context);
        this.tenantId = tenantId;
    }

    @Override
    public JnksIotActorId createActorId() {
        return new JnksIotStringActorId("CFM|" + tenantId);
    }

    @Override
    public JnksIotActor createActor() {
        return new CalculatedFieldManagerActor(context, tenantId);
    }

}
