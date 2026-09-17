/**
 * Copyright © 2016-2025 The Thingsboard Authors
 */
package org.thingsboard.server.transport.udp;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.data.device.profile.UdpTransportFramingMode;
import org.thingsboard.server.transport.udp.session.UdpDeviceSession;
import org.thingsboard.server.transport.udp.util.UdpPayloadUtil;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

@RequiredArgsConstructor
@Slf4j
public class UdpInboundHandler extends SimpleChannelInboundHandler<DatagramPacket> {

    private final UdpTransportContext udpTransportContext;
    private final UdpTransportService udpTransportService;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) {
        InetSocketAddress sender = packet.sender();
        InetSocketAddress local = (InetSocketAddress) ctx.channel().localAddress();
        int localPort = local.getPort();
        byte[] data = new byte[packet.content().readableBytes()];
        packet.content().readBytes(data);
        if (data.length > udpTransportService.getMaxDatagramLength()) {
            log.warn("UDP datagram too large from {} on port {}: {} bytes", sender, localPort, data.length);
            return;
        }
        UdpDeviceSession session = udpTransportContext.resolveOrCreateInboundSession(ctx.channel(), localPort, sender);
        try {
            if (!session.isCoreSessionReady()) {
                if (session.isDeferredPayloadWireAuth()) {
                    udpTransportContext.completeDeferredWireAuthServerAuth(ctx, session, data);
                    return;
                }
                if (udpTransportContext.startServerWireAuth(ctx, session, sender)) {
                    return;
                }
                // 共享端口下鉴权前不知道档案：命中已配置的"延迟鉴权键"时走延迟鉴权。
                if (tryDeferredAuthFromCatalog(ctx, session, data)) {
                    return;
                }
                // 既没有已绑定的延迟鉴权档案、也匹配不上任何延迟鉴权键：身份无从确定，丢弃这一包。
                // 不关连接（UDP 本也无连接）——延迟鉴权目录是异步刷新的，目录热起来后设备重发的包仍能被识别。
                if (session.shouldLogPreAuthDrop()) {
                    log.warn("[{}] UDP pre-auth datagram dropped: no deferred device-id key matched",
                            session.getSessionId());
                } else {
                    log.debug("[{}] UDP pre-auth datagram dropped: no deferred device-id key matched",
                            session.getSessionId());
                }
                return;
            }
            udpTransportContext.recordUplinkFrameActivity(session);
            UdpTransportFramingMode framing = session.getInboundPipelineFramingMode() != null
                    ? session.getInboundPipelineFramingMode()
                    : session.getUdpTransportFramingMode();
            int fixedLen = session.getInboundPipelineFixedFrameLength() > 0
                    ? session.getInboundPipelineFixedFrameLength()
                    : session.getUdpFixedFrameLengthForFraming();
            byte[] frame = UdpPayloadUtil.extractSingleFrame(data, framing, fixedLen, udpTransportService.getMaxDatagramLength());
            if (frame == null) {
                log.warn("[{}] Invalid UDP frame from {}", session.getSessionId(), sender);
                return;
            }
            String jsonPayload = UdpPayloadUtil.decodePayloadBytes(session.getPayloadDataType(), frame);
            session.processIncomingJsonLine(jsonPayload);
        } catch (Exception e) {
            log.warn("[{}] Bad UDP datagram from {}", session.getSessionId(), sender, e);
        }
    }

    /** 命中"已配置的延迟鉴权键"时走延迟鉴权并返回 true；否则返回 false 交给 NONE 路径。 */
    private boolean tryDeferredAuthFromCatalog(ChannelHandlerContext ctx, UdpDeviceSession session, byte[] data) {
        try {
            String authJson = new String(data, StandardCharsets.UTF_8).trim();
            if (!authJson.startsWith("{")) {
                return false;
            }
            JsonObject root = JsonParser.parseString(authJson).getAsJsonObject();
            var deferred = udpTransportContext.getDeferredAuthCatalog().match(root);
            if (deferred.isEmpty()) {
                return false;
            }
            udpTransportContext.completeDeferredWireAuthServerAuth(ctx, session, data, deferred.get().profile());
            return true;
        } catch (Exception e) {
            log.debug("[{}] deferred auth catalog lookup skipped: {}", session.getSessionId(), e.getMessage());
            return false;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("UDP exception on {}", ctx.channel().localAddress(), cause);
    }
}
