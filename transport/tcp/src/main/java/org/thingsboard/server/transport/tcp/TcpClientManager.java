//package org.thingsboard.server.transport.tcp;
//
//import io.netty.bootstrap.Bootstrap;
//import io.netty.channel.Channel;
//import io.netty.channel.ChannelFuture;
//import io.netty.channel.ChannelInitializer;
//import io.netty.channel.EventLoopGroup;
//import io.netty.channel.nio.NioEventLoopGroup;
//import io.netty.channel.socket.SocketChannel;
//import io.netty.channel.socket.nio.NioSocketChannel;
//import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
//import io.netty.handler.codec.LengthFieldPrepender;
//import io.netty.handler.codec.bytes.ByteArrayDecoder;
//import io.netty.handler.codec.bytes.ByteArrayEncoder;
//import io.netty.handler.ssl.SslHandler;
//import io.netty.handler.timeout.IdleStateHandler;
//import lombok.extern.slf4j.Slf4j;
//import org.thingsboard.server.common.data.device.profile.TcpDeviceProfileConfiguration;
//import org.thingsboard.server.common.transport.TransportService;
//import org.thingsboard.server.transport.tcp.TcpTransportContext;
//import org.thingsboard.server.transport.tcp.handler.TcpClientHandler;
//import org.thingsboard.server.transport.tcp.util.TcpDeviceConnectionInfo;
//
//import javax.net.ssl.SSLEngine;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
//
//@Slf4j
//public class TcpClientManager {
//
//    private final TcpTransportContext context;
//    private final TransportService transportService;
//    private final EventLoopGroup workerGroup;
//    private final Map<String, TcpClientHandler> activeConnections;
//    private final AtomicInteger connectionCounter;
//
//    public TcpClientManager(TcpTransportContext context) {
//        this.context = context;
//        this.transportService = context.getTransportService();
//        this.workerGroup = new NioEventLoopGroup(4);
//        this.activeConnections = new ConcurrentHashMap<>();
//        this.connectionCounter = new AtomicInteger(0);
//    }
//
//    public void init() {
//        log.info("TCP Client Manager initialized");
//    }
//
//    public void connectDevice(TcpDeviceConnectionInfo connectionInfo) {
//        String deviceId = connectionInfo.getDeviceId().toString();
//
//        if (activeConnections.containsKey(deviceId)) {
//            log.warn("[{}] Device already connected", deviceId);
//            return;
//        }
//
//        try {
//            Bootstrap b = new Bootstrap();
//            b.group(workerGroup)
//             .channel(NioSocketChannel.class)
//             .handler(new ChannelInitializer<SocketChannel>() {
//                 @Override
//                 protected void initChannel(SocketChannel ch) throws Exception {
//                     // 添加SSL处理器（如果启用）
//                     if (connectionInfo.isSslEnabled()) {
//                         SSLEngine engine = connectionInfo.createSslEngine();
//                         engine.setUseClientMode(true);
//                         ch.pipeline().addLast("ssl", new SslHandler(engine));
//                     }
//
//                     // 空闲状态检测
//                     ch.pipeline().addLast("idleStateHandler",
//                         new IdleStateHandler(0, 0, 60, TimeUnit.SECONDS));
//
//                     // 处理粘包/拆包
//                     ch.pipeline().addLast("frameDecoder",
//                         new LengthFieldBasedFrameDecoder(
//                             context.getMaxPayloadSize(),
//                             0, 4, 0, 4));
//                     ch.pipeline().addLast("frameEncoder",
//                         new LengthFieldPrepender(4));
//
//                     // 字节数组编解码器
//                     ch.pipeline().addLast("bytesDecoder", new ByteArrayDecoder());
//                     ch.pipeline().addLast("bytesEncoder", new ByteArrayEncoder());
//
//                     // 客户端处理器
//                     TcpClientHandler clientHandler = new TcpClientHandler(
//                         context, connectionInfo);
//                     ch.pipeline().addLast("clientHandler", clientHandler);
//
//                     activeConnections.put(deviceId, clientHandler);
//                 }
//             });
//
//            log.info("[{}] Connecting to {}:{}",
//                deviceId, connectionInfo.getHost(), connectionInfo.getPort());
//
//            ChannelFuture future = b.connect(
//                connectionInfo.getHost(),
//                connectionInfo.getPort()
//            ).sync();
//
//            future.addListener(f -> {
//                if (f.isSuccess()) {
//                    connectionCounter.incrementAndGet();
//                    log.info("[{}] Connected successfully", deviceId);
//
//                    // 发送认证消息
//                    sendConnectMessage(deviceId, connectionInfo);
//                } else {
//                    log.error("[{}] Failed to connect", deviceId, f.cause());
//                    scheduleReconnect(connectionInfo);
//                }
//            });
//
//        } catch (Exception e) {
//            log.error("[{}] Failed to create connection", deviceId, e);
//            scheduleReconnect(connectionInfo);
//        }
//    }
//
//    private void sendConnectMessage(String deviceId, TcpDeviceConnectionInfo connectionInfo) {
//        TcpClientHandler handler = activeConnections.get(deviceId);
//        if (handler != null) {
//            handler.sendConnect();
//        }
//    }
//
//    public void disconnectDevice(String deviceId) {
//        TcpClientHandler handler = activeConnections.remove(deviceId);
//        if (handler != null) {
//            handler.disconnect();
//            connectionCounter.decrementAndGet();
//        }
//    }
//
//    public void sendTelemetry(String deviceId, Object telemetry) {
//        TcpClientHandler handler = activeConnections.get(deviceId);
//        if (handler != null) {
//            handler.sendTelemetry(telemetry);
//        } else {
//            log.warn("[{}] Device not connected", deviceId);
//        }
//    }
//
//    public void sendAttributes(String deviceId, Object attributes) {
//        TcpClientHandler handler = activeConnections.get(deviceId);
//        if (handler != null) {
//            handler.sendAttributes(attributes);
//        } else {
//            log.warn("[{}] Device not connected", deviceId);
//        }
//    }
//
//    public void sendRpcRequest(String deviceId, int requestId, String method, Object params) {
//        TcpClientHandler handler = activeConnections.get(deviceId);
//        if (handler != null) {
//            handler.sendRpcRequest(requestId, method, params);
//        } else {
//            log.warn("[{}] Device not connected", deviceId);
//        }
//    }
//
//    private void scheduleReconnect(TcpDeviceConnectionInfo connectionInfo) {
//        if (connectionInfo.getReconnectAttempts() < context.getMaxReconnectAttempts()) {
//            log.info("[{}] Scheduling reconnect in {} ms",
//                connectionInfo.getDeviceId(),
//                context.getClientReconnectInterval());
//
//            context.getScheduler().schedule(() -> {
//                connectionInfo.incrementReconnectAttempts();
//                connectDevice(connectionInfo);
//            }, context.getClientReconnectInterval(), TimeUnit.MILLISECONDS);
//        } else {
//            log.error("[{}] Max reconnect attempts reached",
//                connectionInfo.getDeviceId());
//        }
//    }
//
//    public void shutdown() {
//        log.info("Shutting down TCP Client Manager...");
//
//        // 断开所有连接
//        activeConnections.keySet().forEach(this::disconnectDevice);
//
//        if (workerGroup != null) {
//            workerGroup.shutdownGracefully();
//        }
//
//        log.info("TCP Client Manager stopped");
//    }
//
//    public int getActiveConnections() {
//        return connectionCounter.get();
//    }
//}