package com.jnks.iot.server.transport.lwm2m.server.downlink;

import org.eclipse.leshan.core.request.DeleteRequest;
import org.eclipse.leshan.core.response.DeleteResponse;
import com.jnks.iot.server.transport.lwm2m.server.client.LwM2mClient;
import com.jnks.iot.server.transport.lwm2m.server.log.LwM2MTelemetryLogService;

public class JnksIotLwM2MDeleteCallback extends JnksIotLwM2MTargetedCallback<DeleteRequest, DeleteResponse> {

    public JnksIotLwM2MDeleteCallback(LwM2MTelemetryLogService logService, LwM2mClient client, String targetId) {
        super(logService, client, targetId);
    }

}
