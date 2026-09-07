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
import org.thingsboard.server.transport.udp.netty.UdpPipelineBuilder;
import org.thingsboard.server.transport.udp.session.UdpDeviceSession;
import org.thingsboard.server.transport.udp.util.UdpPayloadUtil;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * UDP CLIENT：平台 connect 到设备后，按已连接 DatagramChannel 处理入站报文（逻辑对齐 {@link org.thingsboard.server.transport.tcp.TcpInboundHandler}）。
 */
@RequiredArgsConstructor
@Slf4j
public class UdpClientInboundHandler extends SimpleChannelInboundHandler<DatagramPacket> {

    private final UdpTransportContext udpTransportContext;
    private final UdpDeviceSession session;
    private final UdpTransportService udpTransportService;

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        session.setChannel(ctx.channel());
        InetSocketAddress remote = (InetSocketAddress) ctx.channel().remoteAddress();
        if (remote != null) {
            session.setRemoteAddress(remote);
        }
        udpTransportContext.finishOutboundUdpClientRegistration(session);
        if (session.getDeviceId() != null) {
            udpTransportContext.resetClientReconnectFailureCount(session.getDeviceId());
        }
        if (session.shouldSendWireAuthPayload()) {
            String token = udpTransportContext.getProtoEntityService()
                    .getDeviceCredentialsByDeviceId(session.getDeviceId()).getCredentialsId();
            session.sendAuthFrame(token);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) {
        byte[] data = new byte[packet.content().readableBytes()];
        packet.content().readBytes(data);
        if (data.length > udpTransportService.getMaxDatagramLength()) {
            log.warn("[{}] UDP client datagram too large: {} bytes", session.getSessionId(), data.length);
            return;
        }
        try {
            if (!session.isCoreSessionReady()) {
                log.warn("[{}] UDP client session not ready on read", session.getSessionId());
                return;
            }
            udpTransportContext.recordUplinkFrameActivity(session);
            UdpTransportFramingMode framing = session.getUdpTransportFramingMode();
            int fixedLen = session.getUdpFixedFrameLengthForFraming();
            byte[] frame = UdpPayloadUtil.extractSingleFrame(data, framing, fixedLen, udpTransportService.getMaxDatagramLength());
            if (frame == null) {
                log.warn("[{}] Invalid UDP client frame", session.getSessionId());
                return;
            }
            String jsonPayload = UdpPayloadUtil.decodePayloadBytes(session.getPayloadDataType(), frame);
            session.processIncomingJsonLine(jsonPayload);
        } catch (Exception e) {
            log.warn("[{}] Bad UDP client datagram", session.getSessionId(), e);
            ctx.close();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        udpTransportContext.onChannelClosed(session);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("[{}] UDP client exception", session.getSessionId(), cause);
        ctx.close();
    }
}
