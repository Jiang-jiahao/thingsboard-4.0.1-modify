package com.jnks.iot.server.transport.lwm2m.server.downlink.composite;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.leshan.core.request.ReadCompositeRequest;
import org.eclipse.leshan.core.response.ReadCompositeResponse;
import com.jnks.iot.server.transport.lwm2m.server.client.LwM2mClient;
import com.jnks.iot.server.transport.lwm2m.server.downlink.JnksIotLwM2MUplinkTargetedCallback;
import com.jnks.iot.server.transport.lwm2m.server.log.LwM2MTelemetryLogService;
import com.jnks.iot.server.transport.lwm2m.server.uplink.LwM2mUplinkMsgHandler;

@Slf4j
public class JnksIotLwM2MReadCompositeCallback extends JnksIotLwM2MUplinkTargetedCallback<ReadCompositeRequest, ReadCompositeResponse> {

    public JnksIotLwM2MReadCompositeCallback(LwM2mUplinkMsgHandler handler, LwM2MTelemetryLogService logService, LwM2mClient client, String[] versionedIds) {
        super(handler, logService, client, versionedIds);
    }

    @Override
    public void onSuccess(ReadCompositeRequest request, ReadCompositeResponse response) {
        super.onSuccess(request, response);
        handler.onUpdateValueAfterReadCompositeResponse(client.getRegistration(), response);
    }

}
