package com.jnks.iot.server.transport.lwm2m.server.rpc;

import org.eclipse.leshan.core.ResponseCode;
import com.jnks.iot.server.common.transport.TransportService;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.transport.lwm2m.server.client.LwM2mClient;
import com.jnks.iot.server.transport.lwm2m.server.downlink.DownlinkRequestCallback;
import com.jnks.iot.server.transport.lwm2m.server.downlink.JnksIotLwM2MCancelObserveRequest;

public class RpcCancelObserveCallback extends RpcDownlinkRequestCallbackProxy<JnksIotLwM2MCancelObserveRequest, Integer> {

    public RpcCancelObserveCallback(TransportService transportService, LwM2mClient client, TransportProtos.ToDeviceRpcRequestMsg requestMsg, DownlinkRequestCallback<JnksIotLwM2MCancelObserveRequest, Integer> callback) {
        super(transportService, client, requestMsg, callback);
    }

    @Override
    protected void sendRpcReplyOnSuccess(Integer response) {
        reply(LwM2MRpcResponseBody.builder().result(ResponseCode.CONTENT.getName()).value(response.toString()).build());
    }
}
