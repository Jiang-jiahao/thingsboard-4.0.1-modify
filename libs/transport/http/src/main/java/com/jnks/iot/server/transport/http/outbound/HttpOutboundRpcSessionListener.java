package com.jnks.iot.server.transport.http.outbound;

import lombok.RequiredArgsConstructor;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.transport.SessionMsgListener;
import com.jnks.iot.server.common.transport.service.TransportActivityManager;
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
        boolean expired = TransportActivityManager.SESSION_EXPIRED_MESSAGE.equals(sessionCloseNotification.getMessage());
        outboundCtx.getTransportContext().destroySession(outboundCtx);
        // 因"不活动"过期是虚拟会话的**宿命**：它从不报活动，所以每
        // transport.sessions.inactivity_timeout（默认 600 秒）必然被回收一次（已设 expiryExempt 做首选豁免，
        // 这里是兜底）。**要立刻重建** —— 否则 reconcile 又要等一轮，那段窗口里的 RPC 会 NO_ACTIVE_CONNECTION。
        if (expired) {
            outboundCtx.getTransportContext().reestablishNow(outboundCtx.getDevice());
        }
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
