/**
 * Copyright © 2016-2025 The Thingsboard Authors
 */
package org.thingsboard.server.transport.udp;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.ResourceLeakDetector;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.TbTransportService;
import org.thingsboard.server.common.data.device.profile.UdpTransportFramingMode;
import org.thingsboard.server.queue.util.TbUdpTransportComponent;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service("UdpTransportService")
@TbUdpTransportComponent
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
    private final ConcurrentHashMap<Integer, Channel> dedicatedListenChannels = new ConcurrentHashMap<>();
    @Getter
    private EventLoopGroup workerGroup;

    @PostConstruct
    public void init() throws Exception {
        log.info("Setting UDP resource leak detector level to {}", leakDetectorLevel);
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.valueOf(leakDetectorLevel.toUpperCase()));
        int workers = workerGroupThreadCount > 0 ? workerGroupThreadCount : Runtime.getRuntime().availableProcessors();
        workerGroup = new NioEventLoopGroup(workers);
        if (!serverEnabled) {
            log.info("UDP server is disabled (transport.udp.server.enabled=false)");
            return;
        }
        log.info("UDP transport ready; listen ports open only when UDP device profiles configure SERVER udpProfileServerBindPort");
    }

    public synchronized void syncDedicatedPorts(Set<Integer> devicePorts) {
        if (!serverEnabled || workerGroup == null) {
            return;
        }
        Set<Integer> desired = new HashSet<>(devicePorts);
        for (Integer boundPort : new HashSet<>(dedicatedListenChannels.keySet())) {
            if (!desired.contains(boundPort)) {
                Channel ch = dedicatedListenChannels.remove(boundPort);
                if (ch != null) {
                    try {
                        context.closeInboundSessionsOnLocalPort(boundPort);
                        ch.close().sync();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.warn("Interrupted while closing UDP listen on {}", boundPort);
                    }
                    log.info("Stopped UDP dedicated listen on {}", boundPort);
                }
            }
        }
        for (int p : desired) {
            if (dedicatedListenChannels.containsKey(p)) {
                continue;
            }
            try {
                Channel ch = bindDatagramSocket(p);
                dedicatedListenChannels.put(p, ch);
                log.info("UDP dedicated listen bound on {}", ch.localAddress());
            } catch (Exception ex) {
                log.error("Failed to bind UDP dedicated port {} — check privileges / port availability", p, ex);
            }
        }
    }

    private Channel bindDatagramSocket(int bindPort) throws InterruptedException {
        Bootstrap b = new Bootstrap();
        b.group(workerGroup)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_BROADCAST, false)
                .handler(new UdpTransportServerInitializer(context, this));
        return b.bind(host, bindPort).sync().channel();
    }

    public int getPrimaryBindPort() {
        return port;
    }

    @PreDestroy
    public void shutdown() throws InterruptedException {
        log.info("Stopping UDP transport");
        try {
            for (Channel ch : dedicatedListenChannels.values()) {
                try {
                    if (ch.localAddress() instanceof InetSocketAddress isa) {
                        context.closeInboundSessionsOnLocalPort(isa.getPort());
                    }
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
        }
        log.info("UDP transport stopped");
    }

    @Override
    public String getName() {
        return DataConstants.UDP_TRANSPORT_NAME;
    }

    public InetSocketAddress getServerAddress() {
        return serverChannel == null ? null : (InetSocketAddress) serverChannel.localAddress();
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
