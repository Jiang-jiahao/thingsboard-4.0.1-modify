package com.jnks.iot.server.transport.http.pull.session;

import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.transport.SessionMsgListener;
import com.jnks.iot.server.gen.transport.TransportProtos;

import java.util.UUID;

/**
 * HTTP Pull 仅上报遥测，不处理 RPC/属性下行。
 */
public class HttpPullNoOpSessionListener implements SessionMsgListener {

    public static final HttpPullNoOpSessionListener INSTANCE = new HttpPullNoOpSessionListener();

    private HttpPullNoOpSessionListener() {
    }

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
    }

    @Override
    public void onToServerRpcResponse(TransportProtos.ToServerRpcResponseMsg toServerResponse) {
    }

    @Override
    public void onDeviceDeleted(DeviceId deviceId) {
    }
}
