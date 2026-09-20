package com.jnks.iot.server.service.session;

import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.gen.transport.TransportProtos.DeviceSessionsCacheEntry;

/**
 * Created by ashvayka on 29.10.18.
 */
public interface DeviceSessionCacheService {

    DeviceSessionsCacheEntry get(DeviceId deviceId);

    DeviceSessionsCacheEntry put(DeviceId deviceId, DeviceSessionsCacheEntry sessions);

}
