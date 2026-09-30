package com.jnks.iot.server.transport.udp;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.device.profile.UdpTransportFramingMode;
import com.jnks.iot.server.transport.udp.session.UdpDeviceSession;
import com.jnks.iot.server.transport.udp.util.UdpPayloadUtil;

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
        // 前面挂的是**透明绑定**的 L4 网关（nginx stream 的 proxy_bind ... transparent）：网关用设备
        // 自己的源地址做上游 socket 的源地址，因此 socket 对端就是真实设备 —— 不解析任何协议头，
        // 也不需要记住「代理侧对端 → 真实客户端」的映射（设备直连平台时同样成立）。
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
                if (tryDeferredAuthFromCatalog(ctx, session, data, localPort)) {
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

    /**
     * 命中"已配置的延迟鉴权键"时走延迟鉴权并返回 true；否则返回 false 交给 NONE 路径。
     * <p>
     * 命中的档案必须**就是本监听端口声明的档案**：端口与档案一一对应（见
     * {@code UdpListenPortRegistry}），否则同一台设备发往 NONE 档案端口的一帧，
     * 只要负载里带了别的延迟鉴权档案配置的键名，就会被改绑成那个档案的设备。
     */
    private boolean tryDeferredAuthFromCatalog(ChannelHandlerContext ctx, UdpDeviceSession session, byte[] data,
                                               int localPort) {
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
            DeviceProfile matched = deferred.get().profile();
            Integer matchedPort = udpTransportContext.resolveProfileListenPort(matched);
            if (matchedPort == null || matchedPort != localPort) {
                log.warn("[{}] Deferred auth catalog matched profile {} (listen port {}), but this session is on port {};"
                                + " refusing to re-bind across device profiles",
                        session.getSessionId(), matched.getId(), matchedPort, localPort);
                return false;
            }
            udpTransportContext.completeDeferredWireAuthServerAuth(ctx, session, data, matched);
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
