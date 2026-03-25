/**
 * Copyright © 2016-2025 The Thingsboard Authors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.transport.tcp;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.ResourceLeakDetector;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import org.thingsboard.server.common.data.device.profile.TcpTransportFramingMode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.TbTransportService;
import org.thingsboard.server.queue.util.TbTcpTransportComponent;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service("TcpTransportService")
@TbTcpTransportComponent
@Slf4j
public class TcpTransportService implements TbTransportService {
    @Value("${transport.tcp.server.enabled:true}")
    @Getter
    private boolean serverEnabled;
    @Value("${transport.tcp.bind_address:0.0.0.0}")
    private String host;
    @Value("${transport.tcp.bind_port:5683}")
    private int port;
    @Value("${transport.tcp.netty.leak_detector_level:PARANOID}")
    private String leakDetectorLevel;
    @Value("${transport.tcp.netty.boss_group_thread_count:1}")
    private int bossGroupThreadCount;
    @Value("${transport.tcp.netty.worker_group_thread_count:0}")
    private int workerGroupThreadCount;
    @Value("${transport.tcp.netty.so_keep_alive:true}")
    private boolean keepAlive;
    @Value("${transport.tcp.netty.max_frame_length:65536}")
    private int maxFrameLength;


    /**
     * SERVER 入站首帧（鉴权）分帧方式；鉴权成功后可按设备配置文件替换为 {@code tcpTransportFramingMode}。
     */
    @Value("${transport.tcp.server.auth_framing_mode:LINE}")
    private String serverAuthFramingMode;
    /**
     * 当鉴权使用 FIXED_LENGTH 时，首帧字节数（需与设备侧约定一致）。
     */
    @Value("${transport.tcp.server.auth_fixed_frame_length:512}")
    private int serverAuthFixedFrameLength;

    @Autowired
    @Lazy
    private TcpTransportContext context;
    private Channel serverChannel;
    private final ConcurrentHashMap<Integer, Channel> dedicatedListenChannels = new ConcurrentHashMap<>();
    private EventLoopGroup bossGroup;
    @Getter
    private EventLoopGroup workerGroup;

    @PostConstruct
    public void init() throws Exception {
        log.info("Setting TCP resource leak detector level to {}", leakDetectorLevel);
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.valueOf(leakDetectorLevel.toUpperCase()));
        int workers = workerGroupThreadCount > 0 ? workerGroupThreadCount : Runtime.getRuntime().availableProcessors();
        bossGroup = new NioEventLoopGroup(bossGroupThreadCount);
        workerGroup = new NioEventLoopGroup(workers);
        if (!serverEnabled) {
            log.info("TCP server is disabled (transport.tcp.server.enabled=false)");
            return;
        }
        log.info("Starting TCP transport server on {}:{} ...", host, port);
        serverChannel = bindListenSocket(port);
        log.info("TCP transport server listening on {}", serverChannel.localAddress());
    }

    /**
     * 为设备专用 {@code serverBindPort} 增删监听；与 {@link #port} 相同则跳过（已由主 socket 监听）。
     */
    public synchronized void syncDedicatedPorts(Set<Integer> devicePorts) {
        if (!serverEnabled || bossGroup == null) {
            return;
        }
        Set<Integer> desired = new HashSet<>(devicePorts);
        desired.remove(port);
        for (Integer boundPort : new HashSet<>(dedicatedListenChannels.keySet())) {
            if (!desired.contains(boundPort)) {
                Channel ch = dedicatedListenChannels.remove(boundPort);
                if (ch != null) {
                    try {
                        ch.close().sync();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.warn("Interrupted while closing TCP listen on {}", boundPort);
                    }
                    log.info("Stopped TCP dedicated listen on {}", boundPort);
                }
            }
        }
        for (int p : desired) {
            if (dedicatedListenChannels.containsKey(p)) {
                continue;
            }
            try {
                Channel ch = bindListenSocket(p);
                dedicatedListenChannels.put(p, ch);
                log.info("TCP dedicated listen bound on {}", ch.localAddress());
            } catch (Exception ex) {
                log.error("Failed to bind TCP dedicated port {} — check privileges / port availability", p, ex);
            }
        }
    }

    private Channel bindListenSocket(int bindPort) throws InterruptedException {
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new TcpTransportServerInitializer(context, this))
                .childOption(ChannelOption.SO_KEEPALIVE, keepAlive);
        return b.bind(host, bindPort).sync().channel();
    }

    public int getPrimaryBindPort() {
        return port;
    }

    @PreDestroy
    public void shutdown() throws InterruptedException {
        log.info("Stopping TCP transport");
        try {
            for (Channel ch : dedicatedListenChannels.values()) {
                try {
                    ch.close().sync();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
            dedicatedListenChannels.clear();
            if (serverChannel != null) {
                serverChannel.close().sync();
            }
        } finally {
            if (workerGroup != null) {
                workerGroup.shutdownGracefully();
            }
            if (bossGroup != null) {
                bossGroup.shutdownGracefully();
            }
        }
        log.info("TCP transport stopped");
    }

    @Override
    public String getName() {
        return DataConstants.TCP_TRANSPORT_NAME;
    }

    public InetSocketAddress getServerAddress() {
        return serverChannel == null ? null : (InetSocketAddress) serverChannel.localAddress();
    }


    public int getMaxFrameLength() {
        return maxFrameLength;
    }

    public TcpTransportFramingMode getServerAuthFramingMode() {
        return TcpTransportFramingMode.valueOf(serverAuthFramingMode.trim());
    }

    public int getServerAuthFixedFrameLength() {
        return serverAuthFixedFrameLength;
    }
}
