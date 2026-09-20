package com.jnks.iot.server.actors.device;

import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.TbActor;
import com.jnks.iot.server.actors.TbActorId;
import com.jnks.iot.server.actors.TbEntityActorId;
import com.jnks.iot.server.actors.service.ContextBasedCreator;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;

public class DeviceActorCreator extends ContextBasedCreator {

    private final TenantId tenantId;
    private final DeviceId deviceId;

    public DeviceActorCreator(ActorSystemContext context, TenantId tenantId, DeviceId deviceId) {
        super(context);
        this.tenantId = tenantId;
        this.deviceId = deviceId;
    }

    @Override
    public TbActorId createActorId() {
        return new TbEntityActorId(deviceId);
    }

    @Override
    public TbActor createActor() {
        return new DeviceActor(context, tenantId, deviceId);
    }

}
