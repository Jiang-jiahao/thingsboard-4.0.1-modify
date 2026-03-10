//package org.thingsboard.server.transport.tcp;
//
//import io.netty.bootstrap.ServerBootstrap;
//import io.netty.channel.Channel;
//import io.netty.channel.ChannelFuture;
//import io.netty.channel.ChannelInitializer;
//import io.netty.channel.ChannelOption;
//import io.netty.channel.EventLoopGroup;
//import io.netty.channel.nio.NioEventLoopGroup;
//import io.netty.channel.socket.SocketChannel;
//import io.netty.channel.socket.nio.NioServerSocketChannel;
//import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
//import io.netty.handler.codec.LengthFieldPrepender;
//import io.netty.handler.codec.bytes.ByteArrayDecoder;
//import io.netty.handler.codec.bytes.ByteArrayEncoder;
//import io.netty.handler.logging.LogLevel;
//import io.netty.handler.logging.LoggingHandler;
//import io.netty.handler.ssl.SslHandler;
//import io.netty.handler.timeout.IdleStateHandler;
//import lombok.extern.slf4j.Slf4j;
//import org.thingsboard.server.transport.tcp.TcpTransportContext;
//import org.thingsboard.server.transport.tcp.handler.TcpTransportHandler;
//import javax.net.ssl.SSLEngine;
//import java.net.InetSocketAddress;
//import java.util.concurrent.TimeUnit;
//
//@Slf4j
//public class TcpServer {
//
//    private final TcpTransportContext context;
//    private EventLoopGroup bossGroup;
//    private EventLoopGroup workerGroup;
//    private Channel channel;
//
//    public TcpServer(TcpTransportContext context) {
//        this.context = context;
//    }
//
//    public void init() {
//        log.info("Starting TCP transport server...");
//
//        bossGroup = new NioEventLoopGroup(context.getBossGroupThreadCount());
//        workerGroup = new NioEventLoopGroup(context.getWorkerGroupThreadCount());
//
//        try {
//            ServerBootstrap b = new ServerBootstrap();
//            b.group(bossGroup, workerGroup)
//             .channel(NioServerSocketChannel.class)
//             .handler(new LoggingHandler(LogLevel.INFO))
//             .childHandler(new ChannelInitializer<SocketChannel>() {
//                 @Override
//                 protected void initChannel(SocketChannel ch) throws Exception {
//                     // 添加SSL处理器（如果启用）
//                     if (context.isSslEnabled() && context.getSslCredentials() != null) {
//                         SSLEngine engine = context.getSslCredentials().createSslEngine();
//                         engine.setUseClientMode(false);
//                         engine.setNeedClientAuth(false);
//                         ch.pipeline().addLast("ssl", new SslHandler(engine));
//                     }
//
//                     // 空闲状态检测（60秒无读写则断开）
//                     ch.pipeline().addLast("idleStateHandler",
//                         new IdleStateHandler(60, 0, 0, TimeUnit.SECONDS));
//
//                     // 处理粘包/拆包：4字节长度字段 + 消息体
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
//                     // 业务处理器
//                     ch.pipeline().addLast("handler",
//                         new TcpTransportHandler(context));
//                 }
//             })
//             .option(ChannelOption.SO_BACKLOG, context.getSoBacklog())
//             .childOption(ChannelOption.SO_KEEPALIVE, context.isSoKeepAlive())
//             .childOption(ChannelOption.TCP_NODELAY, true);
//
//            ChannelFuture future = b.bind(
//                new InetSocketAddress(context.getHost(), context.getPort())
//            ).sync();
//
//            channel = future.channel();
//            log.info("TCP transport server started successfully on {}:{}",
//                context.getHost(), context.getPort());
//
//        } catch (InterruptedException e) {
//            log.error("Failed to start TCP transport server", e);
//            shutdown();
//        }
//    }
//
//    public void shutdown() {
//        log.info("Shutting down TCP transport server...");
//
//        if (channel != null) {
//            channel.close().awaitUninterruptibly();
//        }
//
//        if (workerGroup != null) {
//            workerGroup.shutdownGracefully();
//        }
//
//        if (bossGroup != null) {
//            bossGroup.shutdownGracefully();
//        }
//
//        log.info("TCP transport server stopped");
//    }
//}