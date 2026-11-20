package com.jnks.iot.server.transport.tcp.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * 严格模式：每条入站连接都必须以 PROXY protocol 头开始（入口网关 {@code proxy_protocol on}）。
 * <p>
 * 网关 stream 段是 L4 反代，会终结设备的连接、自己新建到 transport 的连接，内核给的
 * {@code remoteAddress()} 是网关地址。真实源地址只存在于网关写在前面的那行头里。
 */
@Slf4j
public class TcpProxyProtocolHandler extends ChannelInboundHandlerAdapter {

    /** 真实源地址；未经代理的连接不会有这个属性。 */
    public static final AttributeKey<InetSocketAddress> REAL_REMOTE_ADDRESS =
            AttributeKey.valueOf("tcpRealRemoteAddress");

    /** 地址已就绪 —— 依赖源地址的连接期鉴权必须等到此刻才可开始。 */
    public static final class AddressResolved {
        private AddressResolved() {
        }
    }

    private static final AddressResolved ADDRESS_RESOLVED = new AddressResolved();

    /** 真实源地址：经代理时取 PROXY 头解析出的，否则回落到内核给的对端地址。 */
    public static SocketAddress resolve(Channel channel) {
        InetSocketAddress real = channel.attr(REAL_REMOTE_ADDRESS).get();
        return real != null ? real : channel.remoteAddress();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof HAProxyMessage proxyMessage)) {
            ctx.fireChannelRead(msg);
            return;
        }
        try {
            String sourceAddress = proxyMessage.sourceAddress();
            int sourcePort = proxyMessage.sourcePort();
            if (sourceAddress == null || sourcePort <= 0) {
                log.warn("[{}] PROXY header carries no usable source address ({}); closing",
                        ctx.channel().id(), proxyMessage.command());
                ctx.close();
                return;
            }
            ctx.channel().attr(REAL_REMOTE_ADDRESS).set(new InetSocketAddress(sourceAddress, sourcePort));
            ctx.fireUserEventTriggered(ADDRESS_RESOLVED);
        } finally {
            ReferenceCountUtil.release(msg);
            ctx.pipeline().remove(this);
        }
    }
}
