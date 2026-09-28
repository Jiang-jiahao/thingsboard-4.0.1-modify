package com.jnks.iot.server.transport.tcp;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import com.jnks.iot.server.transport.tcp.netty.TcpNettyTransport;
import io.netty.util.ResourceLeakDetector;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import com.jnks.iot.server.common.data.device.profile.TcpTransportFramingMode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import com.jnks.iot.server.common.data.DataConstants;
import com.jnks.iot.server.common.data.JnksIotTransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service("TcpTransportService")
@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.tcp.enabled:true}'=='true'")
@Slf4j
public class TcpTransportService implements JnksIotTransportService {
    @Value("${transport.tcp.server.enabled:true}")
    @Getter
    private boolean serverEnabled;
    @Value("${transport.tcp.bind_address:0.0.0.0}")
    private String host;
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
    /** 本节点当前监听的全部端口：默认共享端口 + 各档案声明的自定义端口。 */
    private final Map<Integer, Channel> listenChannels = new ConcurrentHashMap<>();
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
        // 监听端口全部来自设备档案（见 TcpListenPortRegistry），平台**没有**共享默认端口：
        // 这里先不绑任何端口，启动完成、档案端口加载后由 syncListenPorts 负责绑定。
        log.info("TCP transport has no shared default port; listen ports come from device profiles");
    }

    /**
     * 本节点当前监听的全部端口：即各档案声明的监听端口。
     */
    public Set<Integer> getListenPorts() {
        return Set.copyOf(listenChannels.keySet());
    }

    /**
     * 把监听端口同步为「各档案声明的监听端口集合」。
     * <p>
     * 所有 transport 实例都监听同一组端口（SO_REUSEPORT），不做按档案分片的归属计算，
     * 因此设备连网关/LB 的任一端口都能落到任一实例。每个端口单独 try/catch：重复绑定
     * （NIO 回落下 SO_REUSEPORT 不可用）或端口被占用只让该端口失败，不影响进程启动与其余端口。
     * 集合为空时不监听任何端口（所有档案端口都被删掉时会走到这里）。
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
            // 仅关闭 ServerChannel 不会断开已接受的连接，需先按本地端口关闭该端口上的入站会话。
            context.closeInboundSessionsOnLocalPort(boundPort);
            try {
                ch.close().sync();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            log.info("Stopped TCP listen on {}:{}", host, boundPort);
        }
        for (Integer bindPort : desired) {
            if (bindPort == null || listenChannels.containsKey(bindPort)) {
                continue;
            }
            try {
                Channel ch = bindListenSocket(bindPort);
                listenChannels.put(bindPort, ch);
                log.info("TCP transport listening on {}", ch.localAddress());
            } catch (Exception e) {
                log.error("Failed to bind TCP listen port {}:{} - {}", host, bindPort, e.getMessage());
            }
        }
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
