package com.jnks.iot.server.transport.coap.callback;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.californium.core.coap.CoAP;
import org.eclipse.californium.core.coap.Request;
import org.eclipse.californium.core.server.resources.CoapExchange;
import com.jnks.iot.server.common.adaptor.AdaptorException;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.transport.coap.client.JnksIotCoapClientState;

@Slf4j
public class ToServerRpcSyncSessionCallback extends AbstractSyncSessionCallback {

    public ToServerRpcSyncSessionCallback(JnksIotCoapClientState state, CoapExchange exchange, Request request) {
        super(state, exchange, request);
    }

    @Override
    public void onToServerRpcResponse(TransportProtos.ToServerRpcResponseMsg toServerResponse) {
        try {
            respond(state.getAdaptor().convertToPublish(toServerResponse));
        } catch (AdaptorException e) {
            log.trace("Failed to reply due to error", e);
            exchange.respond(CoAP.ResponseCode.INTERNAL_SERVER_ERROR);
        }
    }
}
