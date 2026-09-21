package com.jnks.iot.server.actors.tenant;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.JnksIotActorCtx;
import com.jnks.iot.server.actors.JnksIotActorRef;
import com.jnks.iot.server.actors.JnksIotEntityActorId;
import com.jnks.iot.server.actors.device.DeviceActorCreator;
import com.jnks.iot.server.actors.service.DefaultActorService;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;

@Component
public class DefaultTenantDeviceActorSupport implements TenantDeviceActorSupport {

    private final ActorSystemContext systemContext;

    public DefaultTenantDeviceActorSupport(@Lazy ActorSystemContext systemContext) {
        this.systemContext = systemContext;
    }

    @Override
    public JnksIotActorRef getOrCreateDeviceActor(JnksIotActorCtx ctx, TenantId tenantId, DeviceId deviceId) {
        return ctx.getOrCreateChildActor(new JnksIotEntityActorId(deviceId),
                () -> DefaultActorService.DEVICE_DISPATCHER_NAME,
                () -> new DeviceActorCreator(systemContext, tenantId, deviceId),
                () -> true);
    }
}
