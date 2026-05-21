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
                if (!session.tryBeginServerAuth()) {
                    return;
                }
                String authJson = new String(data, StandardCharsets.UTF_8).trim();
                JsonObject root = JsonParser.parseString(authJson).getAsJsonObject();
                udpTransportContext.getUdpMessageProcessor().processServerSideAuth(session, root,
                        msg -> udpTransportContext.afterSuccessfulAuth(ctx, session, msg));
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

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("UDP exception on {}", ctx.channel().localAddress(), cause);
    }
}
