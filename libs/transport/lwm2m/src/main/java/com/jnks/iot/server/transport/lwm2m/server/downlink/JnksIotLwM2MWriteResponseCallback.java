package com.jnks.iot.server.transport.lwm2m.server.downlink;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.leshan.core.request.WriteRequest;
import org.eclipse.leshan.core.response.WriteResponse;
import com.jnks.iot.server.transport.lwm2m.server.client.LwM2mClient;
import com.jnks.iot.server.transport.lwm2m.server.log.LwM2MTelemetryLogService;
import com.jnks.iot.server.transport.lwm2m.server.uplink.LwM2mUplinkMsgHandler;

@Slf4j
public class JnksIotLwM2MWriteResponseCallback extends JnksIotLwM2MUplinkTargetedCallback<WriteRequest, WriteResponse> {

    public JnksIotLwM2MWriteResponseCallback(LwM2mUplinkMsgHandler handler, LwM2MTelemetryLogService logService, LwM2mClient client, String targetId) {
        super(handler, logService, client, targetId);
    }

    @Override
    public void onSuccess(WriteRequest request, WriteResponse response) {
        super.onSuccess(request, response);
        handler.onWriteResponseOk(client, versionedId, request, response.getCode().getCode());
    }

}
