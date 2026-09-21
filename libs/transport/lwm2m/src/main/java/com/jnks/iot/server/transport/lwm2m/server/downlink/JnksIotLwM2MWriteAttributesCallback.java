package com.jnks.iot.server.transport.lwm2m.server.downlink;

import org.eclipse.leshan.core.request.WriteAttributesRequest;
import org.eclipse.leshan.core.response.WriteAttributesResponse;
import com.jnks.iot.server.transport.lwm2m.server.client.LwM2mClient;
import com.jnks.iot.server.transport.lwm2m.server.log.LwM2MTelemetryLogService;

public class JnksIotLwM2MWriteAttributesCallback extends JnksIotLwM2MTargetedCallback<WriteAttributesRequest, WriteAttributesResponse> {

    public JnksIotLwM2MWriteAttributesCallback(LwM2MTelemetryLogService logService, LwM2mClient client, String targetId) {
        super(logService, client, targetId);
    }

}
