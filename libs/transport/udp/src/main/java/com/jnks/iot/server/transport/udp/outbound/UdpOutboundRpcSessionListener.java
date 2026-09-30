package com.jnks.iot.server.transport.udp.outbound;

import lombok.RequiredArgsConstructor;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.transport.SessionMsgListener;
import com.jnks.iot.server.gen.transport.TransportProtos;

import java.util.UUID;

/**
 * 出站会话的监听器：只关心"下发 RPC"这一件事。
 * <p>
 * 其余回调（属性、关闭命令等）本就不适用于一个"设备还没开口"的会话，空实现。
 * 唯一的例外是 Core 要求关会话：设备真开口以后 Core 会因并发上限踢掉本会话，
 * 那时必须把它从本地摘掉，否则 reconcile 会一直以为它还活着。
 */
@RequiredArgsConstructor
public class UdpOutboundRpcSessionListener implements SessionMsgListener {

    private final UdpOutboundSessionContext ctx;

    @Override
    public void onToDeviceRpcRequest(UUID sessionId, TransportProtos.ToDeviceRpcRequestMsg toDeviceRequest) {
        ctx.getTransportContext().sendRpcWithoutSession(ctx, toDeviceRequest);
    }

    @Override
    public void onRemoteSessionCloseCommand(UUID sessionId, TransportProtos.SessionCloseNotificationProto sessionCloseNotification) {
        ctx.getTransportContext().destroySession(ctx);
    }

    @Override
    public void onGetAttributesResponse(TransportProtos.GetAttributeResponseMsg getAttributesResponse) {
    }

    @Override
    public void onAttributeUpdate(UUID sessionId, TransportProtos.AttributeUpdateNotificationMsg attributeUpdateNotification) {
    }

    @Override
    public void onToServerRpcResponse(TransportProtos.ToServerRpcResponseMsg toServerResponse) {
    }

    @Override
    public void onDeviceDeleted(DeviceId deviceId) {
    }
}
