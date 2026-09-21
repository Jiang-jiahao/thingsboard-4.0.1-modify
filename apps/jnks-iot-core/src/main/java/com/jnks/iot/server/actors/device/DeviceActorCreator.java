package com.jnks.iot.server.actors.device;

import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.JnksIotActor;
import com.jnks.iot.server.actors.JnksIotActorId;
import com.jnks.iot.server.actors.JnksIotEntityActorId;
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
    public JnksIotActorId createActorId() {
        return new JnksIotEntityActorId(deviceId);
    }

    @Override
    public JnksIotActor createActor() {
        return new DeviceActor(context, tenantId, deviceId);
    }

}
