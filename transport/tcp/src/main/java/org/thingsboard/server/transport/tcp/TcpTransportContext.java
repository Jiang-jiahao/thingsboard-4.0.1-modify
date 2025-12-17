//package org.thingsboard.server.transport.tcp;
//
//import lombok.Getter;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.stereotype.Component;
//import org.thingsboard.server.common.transport.TransportContext;
//import org.thingsboard.server.common.transport.config.ssl.SslCredentials;
//import org.thingsboard.server.transport.tcp.adaptors.TcpAdaptor;
//import org.thingsboard.server.transport.tcp.adaptors.JsonTcpAdaptor;
//import org.thingsboard.server.transport.tcp.adaptors.ProtoTcpAdaptor;
//import org.thingsboard.server.transport.tcp.limits.TcpRateLimits;
//import org.thingsboard.server.transport.tcp.server.TcpServer;
//import org.thingsboard.server.transport.tcp.client.TcpClientManager;
//
//import javax.annotation.PostConstruct;
//import javax.annotation.PreDestroy;
//import java.util.concurrent.atomic.AtomicInteger;
//
//@Slf4j
//@Component
//public class TcpTransportContext extends TransportContext {
//
//    @Getter
//    @Value("${transport.tcp.bind_address:0.0.0.0}")
//    private String host;
//
//    @Getter
//    @Value("${transport.tcp.bind_port:1883}")
//    private int port;
//
//    @Getter
//    @Value("${transport.tcp.ssl.enabled:false}")
//    private boolean sslEnabled;
//
//    @Getter
//    @Value("${transport.tcp.ssl.protocol:TLS}")
//    private String sslProtocol;
//
//    @Getter
//    private SslCredentials sslCredentials;
//
//    @Getter
//    @Value("${transport.tcp.netty.boss_group_thread_count:1}")
//    private int bossGroupThreadCount;
//
//    @Getter
//    @Value("${transport.tcp.netty.worker_group_thread_count:8}")
//    private int workerGroupThreadCount;
//
//    @Getter
//    @Value("${transport.tcp.netty.so_keepalive:true}")
//    private boolean soKeepAlive;
//
//    @Getter
//    @Value("${transport.tcp.netty.so_backlog:4096}")
//    private int soBacklog;
//
//    @Getter
//    @Value("${transport.tcp.msg_queue_size_per_device_limit:100}")
//    private int messageQueueSizePerDeviceLimit;
//
//    @Getter
//    @Value("${transport.tcp.timeout:10000}")
//    private long timeout;
//
//    @Getter
//    @Value("${transport.tcp.max_payload_size:65535}")
//    private Integer maxPayloadSize;
//
//    @Getter
//    @Value("${transport.tcp.client.enabled:false}")
//    private boolean clientEnabled;
//
//    @Getter
//    @Value("${transport.tcp.client.reconnect_interval:30000}")
//    private long clientReconnectInterval;
//
//    @Getter
//    @Value("${transport.tcp.client.max_reconnect_attempts:10}")
//    private int maxReconnectAttempts;
//
//    @Getter
//    private TcpServer tcpServer;
//
//    @Getter
//    private TcpClientManager clientManager;
//
//    private final AtomicInteger connectionsCounter = new AtomicInteger();
//
//    @PostConstruct
//    public void init() {
//        super.init();
//
//        // 初始化SSL证书
//        if (sslEnabled) {
//            try {
//                sslCredentials = new SslCredentials(
//                    "TCP Transport SSL",
//                    context.getSslCredentialsConfig().getSslCredentials()
//                );
//                log.info("TCP SSL credentials loaded successfully");
//            } catch (Exception e) {
//                log.error("Failed to load TCP SSL credentials", e);
//                sslEnabled = false;
//            }
//        }
//
//        // 创建TCP服务器
//        tcpServer = new TcpServer(this);
//        tcpServer.init();
//
//        // 创建TCP客户端管理器（用于连接外部设备）
//        if (clientEnabled) {
//            clientManager = new TcpClientManager(this);
//            clientManager.init();
//        }
//
//        transportService.createGaugeStats("tcp.openConnections", connectionsCounter);
//    }
//
//    public void channelRegistered() {
//        connectionsCounter.incrementAndGet();
//    }
//
//    public void channelUnregistered() {
//        connectionsCounter.decrementAndGet();
//    }
//
//    @PreDestroy
//    public void shutdown() {
//        if (tcpServer != null) {
//            tcpServer.shutdown();
//        }
//        if (clientManager != null) {
//            clientManager.shutdown();
//        }
//    }
//
//    public TcpAdaptor getAdaptor(String type) {
//        if ("PROTOBUF".equalsIgnoreCase(type)) {
//            return new ProtoTcpAdaptor();
//        } else {
//            return new JsonTcpAdaptor();
//        }
//    }
//
//    public TcpRateLimits getRateLimits() {
//        return new TcpRateLimits(this);
//    }
//}