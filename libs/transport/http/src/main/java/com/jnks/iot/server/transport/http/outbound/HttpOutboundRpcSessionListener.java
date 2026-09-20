package com.jnks.iot.server.transport.http.outbound;

import lombok.RequiredArgsConstructor;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.transport.SessionMsgListener;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.transport.http.pull.HttpPullRpcService;

import java.util.UUID;

@RequiredArgsConstructor
public class HttpOutboundRpcSessionListener implements SessionMsgListener {

    private final HttpPullRpcService rpcService;
    private final HttpOutboundSessionContext outboundCtx;

    @Override
    public void onGetAttributesResponse(TransportProtos.GetAttributeResponseMsg getAttributesResponse) {
    }

    @Override
    public void onAttributeUpdate(UUID sessionId, TransportProtos.AttributeUpdateNotificationMsg attributeUpdateNotification) {
    }

    @Override
    public void onRemoteSessionCloseCommand(UUID sessionId, TransportProtos.SessionCloseNotificationProto sessionCloseNotification) {
    }

    @Override
    public void onToDeviceRpcRequest(UUID sessionId, TransportProtos.ToDeviceRpcRequestMsg toDeviceRequest) {
        rpcService.onToDeviceRpcRequest(outboundCtx, toDeviceRequest);
    }

    @Override
    public void onToServerRpcResponse(TransportProtos.ToServerRpcResponseMsg toServerResponse) {
    }

    @Override
    public void onDeviceDeleted(DeviceId deviceId) {
    }
}
