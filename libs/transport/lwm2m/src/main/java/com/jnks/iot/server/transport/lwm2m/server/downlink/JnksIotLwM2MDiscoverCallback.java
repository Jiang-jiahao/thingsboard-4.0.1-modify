package com.jnks.iot.server.transport.lwm2m.server.downlink;

import org.eclipse.leshan.core.request.DiscoverRequest;
import org.eclipse.leshan.core.response.DiscoverResponse;
import com.jnks.iot.server.transport.lwm2m.server.client.LwM2mClient;
import com.jnks.iot.server.transport.lwm2m.server.log.LwM2MTelemetryLogService;

public class JnksIotLwM2MDiscoverCallback extends JnksIotLwM2MTargetedCallback<DiscoverRequest, DiscoverResponse> {

    public JnksIotLwM2MDiscoverCallback(LwM2MTelemetryLogService logService, LwM2mClient client, String targetId) {
        super(logService, client, targetId);
    }

}
