package org.thingsboard.server.actors.tenant;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.thingsboard.server.actors.ActorSystemContext;
import org.thingsboard.server.actors.TbActorCtx;
import org.thingsboard.server.actors.TbActorRef;
import org.thingsboard.server.actors.TbEntityActorId;
import org.thingsboard.server.actors.device.DeviceActorCreator;
import org.thingsboard.server.actors.service.DefaultActorService;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.TenantId;

@Component
public class DefaultTenantDeviceActorSupport implements TenantDeviceActorSupport {

    private final ActorSystemContext systemContext;

    public DefaultTenantDeviceActorSupport(@Lazy ActorSystemContext systemContext) {
        this.systemContext = systemContext;
    }

    @Override
    public TbActorRef getOrCreateDeviceActor(TbActorCtx ctx, TenantId tenantId, DeviceId deviceId) {
        return ctx.getOrCreateChildActor(new TbEntityActorId(deviceId),
                () -> DefaultActorService.DEVICE_DISPATCHER_NAME,
                () -> new DeviceActorCreator(systemContext, tenantId, deviceId),
                () -> true);
    }
}
