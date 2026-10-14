package com.jnks.iot.server.transport.tcp;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.transport.tcp.session.TcpDeviceSession;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

@RequiredArgsConstructor
@Slf4j
public class TcpInboundHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private final TcpTransportContext tcpTransportContext;
    private final TcpDeviceSession session;
    /**
     * true：平台主动连接设备（CLIENT）；false：设备连入平台（SERVER）。
     */
    private final boolean outboundClient;
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            if (e.state() == IdleState.READER_IDLE) {
                log.info("[{}] TCP read idle timeout, closing channel", session.getSessionId());
                ctx.close();
                return;
            }
        }
        ctx.fireUserEventTriggered(evt);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        session.setChannel(ctx.channel());
        if (!outboundClient) {
            tcpTransportContext.trackInboundSession(session);
        }
        if (outboundClient) {
            tcpTransportContext.finishOutboundTcpClientRegistration(session);
        }
        if (outboundClient && session.getDeviceId() != null) {
            tcpTransportContext.resetClientReconnectFailureCount(session.getDeviceId());
        }
        if (!outboundClient) {
            ctx.channel().config().setAutoRead(false);
            if (!tcpTransportContext.startServerWireAuth(ctx, session)) {
                ctx.channel().config().setAutoRead(true);
            }
        }
    }
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf buf) {
        byte[] data = new byte[buf.readableBytes()];
        buf.readBytes(data);
        try {
            if (!session.isCoreSessionReady()) {
                if (outboundClient) {
                    log.warn("[{}] Client session not ready on read", session.getSessionId());
                    return;
                }
                if (session.isDeferredPayloadWireAuth()) {
                    tcpTransportContext.completeDeferredWireAuthServerAuth(ctx, session, data);
                    return;
                }
                // 共享端口下鉴权前不知道档案：若帧里出现某个档案配置的"延迟鉴权键"，
                // 就用该档案走延迟鉴权。
                if (tryDeferredAuthFromCatalog(ctx, session, data, localPort(ctx))) {
                    return;
                }
                // 连接建立时没认领成功（设备记录 / 源 IP 绑定刚建好、注册表还没热等）：逐帧再试一次
                // 按源 IP 认领，并把这一帧扣下、等鉴权成功后补处理 —— 否则同一连接上的数据会一直丢，
                // 只有断开重连才能恢复（UDP 侧逐报文重试天然自愈）。
                if (session.tryBeginServerAuth()) {
                    session.enqueueUnauthFrame(data);
                    if (tcpTransportContext.startServerWireAuth(ctx, session)) {
                        return;
                    }
                    session.clearPendingUnauthFrames();
                    session.endServerAuth();
                } else {
                    // 鉴权正在途中：这一帧同样不能丢 —— 入队，等鉴权成功后一起补处理。
                    // （原先这里直接丢弃，实测在"设备先连、记录后建"场景下会稳定丢帧。）
                    session.enqueueUnauthFrame(data);
                }
                // 身份此刻仍无从确定。已入队的帧若鉴权最终失败会随会话一起丢弃；不关连接 ——
                // 延迟鉴权目录是异步刷新的，目录热起来后设备重发的帧仍能被识别。
                if (session.shouldLogPreAuthDrop()) {
                    log.warn("[{}] TCP pre-auth frame queued (no device for this source IP yet, or wire auth in flight)",
                            session.getSessionId());
                } else {
                    log.debug("[{}] TCP pre-auth frame queued (no device for this source IP yet, or wire auth in flight)",
                            session.getSessionId());
                }
                return;
            }
            tcpTransportContext.processUplinkFrame(session, data);
        } catch (Exception e) {
            log.warn("[{}] Bad TCP frame", session.getSessionId(), e);
            ctx.close();
        }
    }

    /** 本连接落在服务器哪个监听端口上（档案级监听端口一一对应档案）。 */
    private static int localPort(ChannelHandlerContext ctx) {
        var la = ctx.channel().localAddress();
        return la instanceof InetSocketAddress isa ? isa.getPort() : -1;
    }

    /**
     * 命中"已配置的延迟鉴权键"时走延迟鉴权并返回 true；否则返回 false 交给 token 鉴权路径。
     * <p>
     * 命中的档案必须**就是本监听端口声明的档案**：端口与档案一一对应（见 {@code TcpListenPortRegistry}），
     * 否则发往 NONE 档案端口的一帧，只要负载里带了别的延迟鉴权档案配置的键名，
     * 就会被改绑成那个档案的设备（跨档案越权）。
     */
    private boolean tryDeferredAuthFromCatalog(ChannelHandlerContext ctx, TcpDeviceSession session, byte[] data,
                                               int localPort) {
        try {
            String authJson = new String(data, StandardCharsets.UTF_8).trim();
            if (!authJson.startsWith("{")) {
                return false;
            }
            JsonObject root = JsonParser.parseString(authJson).getAsJsonObject();
            var deferred = tcpTransportContext.getDeferredAuthCatalog().match(root, localPort);
            if (deferred.isEmpty()) {
                return false;
            }
            DeviceProfile matched = deferred.get().profile();
            Integer matchedPort = tcpTransportContext.resolveProfileListenPort(matched);
            if (matchedPort == null || matchedPort != localPort) {
                log.warn("[{}] Deferred auth catalog matched profile {} (listen port {}), but this session is on port {};"
                                + " refusing to re-bind across device profiles",
                        session.getSessionId(), matched.getId(), matchedPort, localPort);
                return false;
            }
            tcpTransportContext.completeDeferredWireAuthServerAuth(ctx, session, data, matched);
            return true;
        } catch (Exception e) {
            log.debug("[{}] deferred auth catalog lookup skipped: {}", session.getSessionId(), e.getMessage());
            return false;
        }
    }
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        tcpTransportContext.onChannelClosed(session, session.takePendingDisconnectCause());
    }
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("[{}] TCP exception", session.getSessionId(), cause);
        session.setPendingDisconnectCause(cause);
        ctx.close();
    }
}