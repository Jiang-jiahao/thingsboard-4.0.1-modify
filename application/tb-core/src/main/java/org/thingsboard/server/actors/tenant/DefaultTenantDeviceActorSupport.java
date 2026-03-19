/**
 * Copyright (c) 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.actors.tenant;

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

    public DefaultTenantDeviceActorSupport(ActorSystemContext systemContext) {
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
