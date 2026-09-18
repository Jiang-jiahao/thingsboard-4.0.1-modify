package org.thingsboard.server.transport.tcp.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueChannelOption;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

/**
 * Netty 传输实现选择：优先原生 epoll（Linux）/ kqueue（macOS、BSD），否则回落到 JDK NIO。
 * <p>
 * 只有原生传输能设置 {@code SO_REUSEPORT}（JDK 的 {@code StandardSocketOptions} 不含该选项），
 * 因此"同一主机上多个实例共享同一 IP:端口"需要原生传输 + 对应平台的 native 库。
 */
@Slf4j
public final class TcpNettyTransport {

    private TcpNettyTransport() {
    }

    public static String name() {
        if (Epoll.isAvailable()) {
            return "epoll";
        }
        if (KQueue.isAvailable()) {
            return "kqueue";
        }
        return "nio";
    }

    public static EventLoopGroup newEventLoopGroup(int threads) {
        if (Epoll.isAvailable()) {
            return new EpollEventLoopGroup(threads);
        }
        if (KQueue.isAvailable()) {
            return new KQueueEventLoopGroup(threads);
        }
        return new NioEventLoopGroup(threads);
    }

    public static Class<? extends ServerChannel> serverChannelClass() {
        if (Epoll.isAvailable()) {
            return EpollServerSocketChannel.class;
        }
        if (KQueue.isAvailable()) {
            return KQueueServerSocketChannel.class;
        }
        return NioServerSocketChannel.class;
    }

    public static Class<? extends SocketChannel> socketChannelClass() {
        if (Epoll.isAvailable()) {
            return EpollSocketChannel.class;
        }
        if (KQueue.isAvailable()) {
            return KQueueSocketChannel.class;
        }
        return NioSocketChannel.class;
    }

    /**
     * 监听 socket 上按需开启 {@code SO_REUSEPORT}；NIO 无法设置时给出一次性告警。
     */
    public static ServerBootstrap applyReusePort(ServerBootstrap bootstrap, boolean reusePort) {
        if (!reusePort) {
            return bootstrap;
        }
        if (Epoll.isAvailable()) {
            return bootstrap.option(EpollChannelOption.SO_REUSEPORT, true);
        }
        if (KQueue.isAvailable()) {
            return bootstrap.option(KQueueChannelOption.SO_REUSEPORT, true);
        }
        log.warn("transport.tcp.reuse_port=true but the transport runs on NIO (no epoll/kqueue native library); "
                + "multiple TCP transport instances on the same host cannot share the same port");
        return bootstrap;
    }

    /**
     * CLIENT 出站连接不走 SO_REUSEPORT，但同样需要与监听侧一致的传输实现。
     */
    public static Bootstrap applyClientChannel(Bootstrap bootstrap) {
        return bootstrap.channel(socketChannelClass());
    }
}
