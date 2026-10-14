package com.jnks.iot.server.transport.http.pull.session;

import lombok.RequiredArgsConstructor;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.transport.SessionMsgListener;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.transport.http.pull.HttpPullRpcService;

import java.util.UUID;

@RequiredArgsConstructor
public class HttpPullRpcSessionListener implements SessionMsgListener {

    private final HttpPullRpcService rpcService;
    private final HttpPullCollectorSessionContext collectorCtx;

    @Override
    public void onGetAttributesResponse(TransportProtos.GetAttributeResponseMsg getAttributesResponse) {
    }

    @Override
    public void onAttributeUpdate(UUID sessionId, TransportProtos.AttributeUpdateNotificationMsg attributeUpdateNotification) {
    }

    @Override
    public void onRemoteSessionCloseCommand(UUID sessionId, TransportProtos.SessionCloseNotificationProto sessionCloseNotification) {
        // Core 会话超时 / 传输层不活动过期都会走这里。采集会话没有"断开重连"的机会：
        // 不处理的话 ctx 会一直挂着一个已死的 sessionInfo —— 平台继续拉、数据也落库，
        // 但活动上报因为会话已不在传输层会话表里发不出去，设备会显示「非活跃」。
        // 交给 context 用新 sessionId 重建（见 HttpPullTransportContext#onTransportSessionClosed）。
        if (collectorCtx != null && collectorCtx.getTransportContext() != null) {
            collectorCtx.getTransportContext().onTransportSessionClosed(collectorCtx, sessionId);
        }
    }

    @Override
    public void onToDeviceRpcRequest(UUID sessionId, TransportProtos.ToDeviceRpcRequestMsg toDeviceRequest) {
        rpcService.onToDeviceRpcRequest(collectorCtx, toDeviceRequest);
    }

    @Override
    public void onToServerRpcResponse(TransportProtos.ToServerRpcResponseMsg toServerResponse) {
    }

    @Override
    public void onDeviceDeleted(DeviceId deviceId) {
    }
}
