/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.udp.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueChannelOption;
import io.netty.channel.kqueue.KQueueDatagramChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import lombok.extern.slf4j.Slf4j;

/**
 * UDP 侧的 Netty 传输实现选择，与 {@code TcpNettyTransport} 同构：
 * 优先原生 epoll（Linux）/ kqueue（macOS、BSD），否则回落 NIO。
 * <p>
 * 同主机多实例共享同一 UDP 端口同样需要 {@code SO_REUSEPORT}：内核按四元组分派数据报，
 * 同一源地址+源端口的数据报会稳定落在同一实例上，因此会话不会跨实例漂移。
 */
@Slf4j
public final class UdpNettyTransport {

    private UdpNettyTransport() {
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

    public static Class<? extends DatagramChannel> datagramChannelClass() {
        if (Epoll.isAvailable()) {
            return EpollDatagramChannel.class;
        }
        if (KQueue.isAvailable()) {
            return KQueueDatagramChannel.class;
        }
        return NioDatagramChannel.class;
    }

    /**
     * 监听 socket 上按需开启 {@code SO_REUSEPORT}；NIO 无法设置时给出一次性告警。
     */
    public static Bootstrap applyReusePort(Bootstrap bootstrap, boolean reusePort) {
        if (!reusePort) {
            return bootstrap;
        }
        if (Epoll.isAvailable()) {
            return bootstrap.option(EpollChannelOption.SO_REUSEPORT, true);
        }
        if (KQueue.isAvailable()) {
            return bootstrap.option(KQueueChannelOption.SO_REUSEPORT, true);
        }
        log.warn("transport.udp.reuse_port=true but the transport runs on NIO (no epoll/kqueue native library); "
                + "multiple UDP transport instances on the same host cannot share the same port");
        return bootstrap;
    }
}
