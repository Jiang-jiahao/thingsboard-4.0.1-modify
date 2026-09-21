package com.jnks.iot.server.transport.lwm2m.server.downlink.composite;

import org.eclipse.leshan.core.request.WriteCompositeRequest;
import org.eclipse.leshan.core.response.WriteCompositeResponse;
import com.jnks.iot.server.transport.lwm2m.server.client.LwM2mClient;
import com.jnks.iot.server.transport.lwm2m.server.downlink.JnksIotLwM2MUplinkTargetedCallback;
import com.jnks.iot.server.transport.lwm2m.server.log.LwM2MTelemetryLogService;
import com.jnks.iot.server.transport.lwm2m.server.uplink.LwM2mUplinkMsgHandler;

public class JnksIotLwM2MWriteResponseCompositeCallback extends JnksIotLwM2MUplinkTargetedCallback<WriteCompositeRequest, WriteCompositeResponse> {

    public JnksIotLwM2MWriteResponseCompositeCallback(LwM2mUplinkMsgHandler handler, LwM2MTelemetryLogService logService, LwM2mClient client, String targetId) {
        super(handler, logService, client, targetId);
    }

    @Override
    public void onSuccess(WriteCompositeRequest request, WriteCompositeResponse response) {
        super.onSuccess(request, response);
        handler.onWriteCompositeResponseOk(client, request, response.getCode().getCode());
    }

}
