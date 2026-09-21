package com.jnks.iot.server.transport.lwm2m.server.downlink;

import org.eclipse.leshan.core.request.ExecuteRequest;
import org.eclipse.leshan.core.response.ExecuteResponse;
import com.jnks.iot.server.transport.lwm2m.server.client.LwM2mClient;
import com.jnks.iot.server.transport.lwm2m.server.log.LwM2MTelemetryLogService;

public class JnksIotLwM2MExecuteCallback extends JnksIotLwM2MTargetedCallback<ExecuteRequest, ExecuteResponse> {

    public JnksIotLwM2MExecuteCallback(LwM2MTelemetryLogService logService, LwM2mClient client, String targetId) {
        super(logService, client, targetId);
    }

}
