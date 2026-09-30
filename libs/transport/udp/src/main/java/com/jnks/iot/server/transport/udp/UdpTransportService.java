package com.jnks.iot.server.transport.udp;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import com.jnks.iot.server.transport.udp.netty.UdpNettyTransport;
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
import com.jnks.iot.server.common.data.DataConstants;
import com.jnks.iot.server.common.data.JnksIotTransportService;
import com.jnks.iot.server.common.data.device.profile.UdpTransportFramingMode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service("UdpTransportService")
@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.udp.enabled:true}'=='true'")
@Slf4j
public class UdpTransportService implements JnksIotTransportService {

    @Value("${transport.udp.server.enabled:true}")
    @Getter
    private boolean serverEnabled;
    @Value("${transport.udp.bind_address:0.0.0.0}")
    private String host;
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

    /** 本节点当前监听的全部端口：即各档案声明的监听端口。 */
    private final Map<Integer, Channel> listenChannels = new ConcurrentHashMap<>();
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
        // 监听端口全部来自设备档案（见 UdpListenPortRegistry），平台**没有**共享默认端口：
        // 这里先不绑任何端口，启动完成、档案端口加载后由 syncListenPorts 负责绑定。
        log.info("UDP transport has no shared default port; listen ports come from device profiles");
    }

    /**
     * 本节点当前监听的全部端口：即各档案声明的监听端口。
     */
    public Set<Integer> getListenPorts() {
        return Set.copyOf(listenChannels.keySet());
    }

    /**
     * 取某个监听端口对应的通道；没绑定该端口返回 {@code null}。
     * <p>
     * 出站会话在**设备还没有会话**时用它把下行发出去：从平台监听的端口发，设备回包才会落回同一个端口
     * （见 {@code com.jnks.iot.server.transport.udp.outbound}）。
     */
    public Channel getListenChannel(int port) {
        return listenChannels.get(port);
    }

    /**
     * 把监听端口同步为「默认共享端口 ∪ 档案声明的自定义端口」。
     * <p>
     * 所有 transport 实例都监听同一组端口（SO_REUSEPORT），不做按档案分片的归属计算，
     * 因此设备把数据报发到网关/LB 的任一端口都能落到任一实例。每个端口单独 try/catch。
     */
    public synchronized void syncListenPorts(Collection<Integer> profilePorts) {
        if (!serverEnabled) {
            return;
        }
        Set<Integer> desired = new HashSet<>(profilePorts);
        for (Integer boundPort : new ArrayList<>(listenChannels.keySet())) {
            if (desired.contains(boundPort)) {
                continue;
            }
            Channel ch = listenChannels.remove(boundPort);
            if (ch == null) {
                continue;
            }
            // 仅关闭监听 socket 不会结束已建立的会话，需先按本地端口关闭该端口上的入站会话。
            context.closeInboundSessionsOnLocalPort(boundPort);
            try {
                ch.close().sync();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            log.info("Stopped UDP listen on {}:{}", host, boundPort);
        }
        for (Integer bindPort : desired) {
            if (bindPort == null || listenChannels.containsKey(bindPort)) {
                continue;
            }
            try {
                Channel ch = bindDatagramSocket(bindPort);
                listenChannels.put(bindPort, ch);
                log.info("UDP transport listening on {}", ch.localAddress());
            } catch (Exception e) {
                log.error("Failed to bind UDP listen port {}:{} - {}", host, bindPort, e.getMessage());
            }
        }
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
            for (Map.Entry<Integer, Channel> entry : listenChannels.entrySet()) {
                // 共享监听端口：本节点退出会切断落在本节点上的全部设备会话，主动上报会话关闭与非活跃，
                // 避免 Core 侧要等 transport.sessions.inactivity_timeout（默认 600s）。
                context.closeInboundSessionsOnLocalPort(entry.getKey());
                entry.getValue().close().sync();
            }
            listenChannels.clear();
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
