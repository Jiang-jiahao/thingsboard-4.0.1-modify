package com.jnks.iot.server.transport.udp.outbound;

import lombok.RequiredArgsConstructor;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.transport.SessionMsgListener;
import com.jnks.iot.server.common.transport.service.TransportActivityManager;
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
        boolean expired = TransportActivityManager.SESSION_EXPIRED_MESSAGE.equals(sessionCloseNotification.getMessage());
        ctx.getTransportContext().destroySession(ctx);
        // 因"不活动"过期是虚拟会话的**宿命**：它从不报活动（有意不伪造设备活跃），所以每
        // transport.sessions.inactivity_timeout（默认 600 秒）必然被回收一次。**要立刻重建** ——
        // 等 30 秒一轮的 reconcile 就会每 10 分钟留出一段没有下发通道的窗口：落进去的 RPC
        // 直接 NO_ACTIVE_CONNECTION，已进队列的则被静默丢弃（传输层只打 debug 级 Missing session）。
        // 线上实测：03:07:58 销毁 → 03:08:26 重建，03:08:16 的 RPC 就是这么失败的。
        if (expired) {
            ctx.getTransportContext().reestablishNow(ctx.getDevice());
        }
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
