package org.thingsboard.server.actors.tenant;

import org.thingsboard.server.actors.TbActorCtx;
import org.thingsboard.server.actors.TbActorRef;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.TenantId;

/**
 * Provides device actor creation for tenant actor without hard dependency on tb-core module.
 */
public interface TenantDeviceActorSupport {

    TbActorRef getOrCreateDeviceActor(TbActorCtx ctx, TenantId tenantId, DeviceId deviceId);

}
