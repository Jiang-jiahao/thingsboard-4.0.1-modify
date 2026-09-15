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
import org.thingsboard.server.transport.tcp.netty.TcpNettyTransport;
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
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.TbTransportService;

import java.net.InetSocketAddress;

@Service("TcpTransportService")
@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.tcp.enabled:true}'=='true'")
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
    @Value("${transport.tcp.reuse_port:true}")
    private boolean reusePort;
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
    private EventLoopGroup bossGroup;
    @Getter
    private EventLoopGroup workerGroup;

    @PostConstruct
    public void init() throws Exception {
        log.info("Setting TCP resource leak detector level to {}", leakDetectorLevel);
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.valueOf(leakDetectorLevel.toUpperCase()));
        int workers = workerGroupThreadCount > 0 ? workerGroupThreadCount : Runtime.getRuntime().availableProcessors();
        log.info("TCP transport uses {} transport, reuse_port={}", TcpNettyTransport.name(), reusePort);
        bossGroup = TcpNettyTransport.newEventLoopGroup(bossGroupThreadCount);
        workerGroup = TcpNettyTransport.newEventLoopGroup(workers);
        if (!serverEnabled) {
            log.info("TCP server is disabled (transport.tcp.server.enabled=false)");
            return;
        }
        log.info("Starting TCP transport server on {}:{} ...", host, port);
        serverChannel = bindListenSocket(port);
        log.info("TCP transport server listening on {}", serverChannel.localAddress());
    }

    private Channel bindListenSocket(int bindPort) throws InterruptedException {
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(TcpNettyTransport.serverChannelClass())
                .childHandler(new TcpTransportServerInitializer(context, this))
                .childOption(ChannelOption.SO_KEEPALIVE, keepAlive);
        TcpNettyTransport.applyReusePort(b, reusePort);
        return b.bind(host, bindPort).sync().channel();
    }

    @PreDestroy
    public void shutdown() throws InterruptedException {
        log.info("Stopping TCP transport");
        try {
            if (serverChannel != null) {
                // 共享监听端口：本节点退出会切断落在本节点上的全部设备会话，主动上报会话关闭与非活跃，
                // 避免 Core 侧要等 transport.sessions.inactivity_timeout（默认 600s）。
                if (serverChannel.localAddress() instanceof InetSocketAddress isa) {
                    context.closeInboundSessionsOnLocalPort(isa.getPort());
                }
                serverChannel.close().sync();
            }
            if (context.getTransportService() != null) {
                context.getTransportService().closeLocalSessionsAndReportInactivity();
                context.getTransportService().flushToCore();
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
