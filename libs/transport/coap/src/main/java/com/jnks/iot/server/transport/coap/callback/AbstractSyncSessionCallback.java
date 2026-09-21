package com.jnks.iot.server.transport.coap.callback;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.californium.core.coap.Request;
import org.eclipse.californium.core.coap.Response;
import org.eclipse.californium.core.server.resources.CoapExchange;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.transport.SessionMsgListener;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.transport.coap.client.JnksIotCoapClientState;
import com.jnks.iot.server.transport.coap.client.JnksIotCoapContentFormatUtil;
import com.jnks.iot.server.transport.coap.client.JnksIotCoapObservationState;

import java.util.UUID;

@RequiredArgsConstructor
@Slf4j
public abstract class AbstractSyncSessionCallback implements SessionMsgListener {

    protected final JnksIotCoapClientState state;
    protected final CoapExchange exchange;
    protected final Request request;

    @Override
    public void onGetAttributesResponse(TransportProtos.GetAttributeResponseMsg getAttributesResponse) {
        logUnsupportedCommandMessage(getAttributesResponse);
    }

    @Override
    public void onAttributeUpdate(UUID sessionId, TransportProtos.AttributeUpdateNotificationMsg attributeUpdateNotification) {
        logUnsupportedCommandMessage(attributeUpdateNotification);
    }

    @Override
    public void onRemoteSessionCloseCommand(UUID sessionId, TransportProtos.SessionCloseNotificationProto sessionCloseNotification) {

    }

    @Override
    public void onDeviceDeleted(DeviceId deviceId) {

    }

    @Override
    public void onToDeviceRpcRequest(UUID sessionId, TransportProtos.ToDeviceRpcRequestMsg toDeviceRequest) {
        logUnsupportedCommandMessage(toDeviceRequest);
    }

    @Override
    public void onToServerRpcResponse(TransportProtos.ToServerRpcResponseMsg toServerResponse) {
        logUnsupportedCommandMessage(toServerResponse);
    }

    private void logUnsupportedCommandMessage(Object update) {
        log.trace("[{}] Ignore unsupported update: {}", state.getDeviceId(), update);
    }

    public static boolean isConRequest(JnksIotCoapObservationState state) {
        if (state != null) {
            return state.getExchange().advanced().getRequest().isConfirmable();
        } else {
            return false;
        }
    }

    protected void respond(Response response) {
        response.getOptions().setContentFormat(JnksIotCoapContentFormatUtil.getContentFormat(exchange.getRequestOptions().getContentFormat(), state.getContentFormat()));
        response.setConfirmable(exchange.advanced().getRequest().isConfirmable());
        exchange.respond(response);
    }

}
