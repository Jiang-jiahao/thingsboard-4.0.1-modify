package com.jnks.iot.server.transport.lwm2m.server.downlink;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.leshan.core.request.ObserveRequest;
import org.eclipse.leshan.core.response.ObserveResponse;
import com.jnks.iot.server.transport.lwm2m.server.client.LwM2mClient;
import com.jnks.iot.server.transport.lwm2m.server.log.LwM2MTelemetryLogService;
import com.jnks.iot.server.transport.lwm2m.server.uplink.LwM2mUplinkMsgHandler;

@Slf4j
public class JnksIotLwM2MObserveCallback extends JnksIotLwM2MUplinkTargetedCallback<ObserveRequest, ObserveResponse> {

    public JnksIotLwM2MObserveCallback(LwM2mUplinkMsgHandler handler, LwM2MTelemetryLogService logService, LwM2mClient client, String targetId) {
        super(handler, logService, client, targetId);
    }

    @Override
    public void onSuccess(ObserveRequest request, ObserveResponse response) {
        super.onSuccess(request, response);
        handler.onUpdateValueAfterReadResponse(client.getRegistration(), versionedId, response);
    }
}
