package com.jnks.iot.server.transport.udp;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.common.data.device.profile.UdpTransportFramingMode;
import com.jnks.iot.server.transport.udp.session.UdpDeviceSession;
import com.jnks.iot.server.transport.udp.util.UdpPayloadUtil;
import com.jnks.iot.server.transport.udp.util.UdpProxyProtocol;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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
        // 前面挂了 L4 代理（nginx stream 的 proxy_protocol on）时，真实源地址在 PROXY v2 头里：
        // 用它替代 socket 对端地址，源 IP 判定（NONE/sourceHost）与下行回包地址都按真实设备走。
        // 没有协议头的数据报原样处理（设备直连平台时就是这样）。
        if (UdpProxyProtocol.looksLikeHeader(data)) {
            UdpProxyProtocol.Header proxyHeader = UdpProxyProtocol.parse(data);
            if (proxyHeader != null) {
                log.debug("UDP PROXY protocol: real source {} on port {} (gateway peer {})",
                        proxyHeader.source(), localPort, packet.sender());
                sender = proxyHeader.source();
                data = Arrays.copyOfRange(data, proxyHeader.payloadOffset(), data.length);
                udpTransportContext.rememberProxiedClient(packet.sender(), sender);
            } else {
                log.warn("UDP datagram from {} on port {} looks like a PROXY protocol header but could not be parsed (head={}); treated as payload",
                        packet.sender(), localPort, headHex(data));
            }
        } else {
            // nginx 的 UDP 代理**只在会话首包**带 PROXY 头：同会话后续数据报没有头，
            // 这里用首包记住的真实客户端地址还原，保证会话键与源 IP 判定在整个会话内一致。
            InetSocketAddress proxiedClient = udpTransportContext.proxiedClientFor(packet.sender());
            if (proxiedClient != null) {
                sender = proxiedClient;
            }
        }
        UdpDeviceSession session = udpTransportContext.resolveOrCreateInboundSession(ctx.channel(), localPort, sender);
        session.setLastUplinkMs(System.currentTimeMillis());   // 读空闲清理据此判断"多久没收到该设备的数据报"
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
                    log.warn("[{}] UDP pre-auth datagram dropped from {} on port {}: no deferred device-id key matched "
                                    + "(no device declares this source IP as its sourceHost, and the payload carries no configured auth key)",
                            session.getSessionId(), sender, localPort);
                } else {
                    log.debug("[{}] UDP pre-auth datagram dropped from {} on port {}: no deferred device-id key matched",
                            session.getSessionId(), sender, localPort);
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

    /** 解析失败时把头部若干字节打进日志，免得只能靠猜。 */
    private static String headHex(byte[] data) {
        int n = Math.min(data.length, 24);
        StringBuilder sb = new StringBuilder(n * 3);
        for (int i = 0; i < n; i++) {
            sb.append(String.format("%02x", data[i]));
        }
        return sb.toString();
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
