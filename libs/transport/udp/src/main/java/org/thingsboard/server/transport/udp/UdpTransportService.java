/**
 * Copyright © 2016-2025 The Thingsboard Authors
 */
package org.thingsboard.server.transport.udp;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import org.thingsboard.server.transport.udp.netty.UdpNettyTransport;
import io.netty.util.ResourceLeakDetector;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.TbTransportService;
import org.thingsboard.server.common.data.device.profile.UdpTransportFramingMode;

import java.net.InetSocketAddress;

@Service("UdpTransportService")
@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.udp.enabled:true}'=='true'")
@Slf4j
public class UdpTransportService implements TbTransportService {

    @Value("${transport.udp.server.enabled:true}")
    @Getter
    private boolean serverEnabled;
    @Value("${transport.udp.bind_address:0.0.0.0}")
    private String host;
    @Value("${transport.udp.bind_port:5684}")
    private int port;
    @Value("${transport.udp.netty.leak_detector_level:PARANOID}")
    private String leakDetectorLevel;
    @Value("${transport.udp.netty.worker_group_thread_count:0}")
    private int workerGroupThreadCount;
    @Value("${transport.udp.reuse_port:true}")
    private boolean reusePort;
    @Value("${transport.udp.netty.max_datagram_length:65536}")
    private int maxDatagramLength;

    @Value("${transport.udp.server.auth_framing_mode:NONE}")
    private String serverAuthFramingMode;
    @Value("${transport.udp.server.auth_fixed_frame_length:512}")
    private int serverAuthFixedFrameLength;

    @Autowired
    @Lazy
    private UdpTransportContext context;

    private Channel serverChannel;
    @Getter
    private EventLoopGroup workerGroup;

    @PostConstruct
    public void init() throws Exception {
        log.info("Setting UDP resource leak detector level to {}", leakDetectorLevel);
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.valueOf(leakDetectorLevel.toUpperCase()));
        int workers = workerGroupThreadCount > 0 ? workerGroupThreadCount : Runtime.getRuntime().availableProcessors();
        log.info("UDP transport uses {} transport, reuse_port={}", UdpNettyTransport.name(), reusePort);
        workerGroup = UdpNettyTransport.newEventLoopGroup(workers);
        if (!serverEnabled) {
            log.info("UDP server is disabled (transport.udp.server.enabled=false)");
            return;
        }
        log.info("Starting UDP transport server on {}:{} ...", host, port);
        serverChannel = bindDatagramSocket(port);
        log.info("UDP transport server listening on {}", serverChannel.localAddress());
    }

    private Channel bindDatagramSocket(int bindPort) throws InterruptedException {
        Bootstrap b = new Bootstrap();
        b.group(workerGroup)
                .channel(UdpNettyTransport.datagramChannelClass())
                .option(ChannelOption.SO_BROADCAST, false)
                .handler(new UdpTransportServerInitializer(context, this));
        UdpNettyTransport.applyReusePort(b, reusePort);
        return b.bind(host, bindPort).sync().channel();
    }

    @PreDestroy
    public void shutdown() throws InterruptedException {
        log.info("Stopping UDP transport");
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
        }
        log.info("UDP transport stopped");
    }

    @Override
    public String getName() {
        return DataConstants.UDP_TRANSPORT_NAME;
    }

    public int getMaxDatagramLength() {
        return maxDatagramLength;
    }

    public UdpTransportFramingMode getServerAuthFramingMode() {
        return UdpTransportFramingMode.valueOf(serverAuthFramingMode.trim());
    }

    public int getServerAuthFixedFrameLength() {
        return serverAuthFixedFrameLength;
    }
}
