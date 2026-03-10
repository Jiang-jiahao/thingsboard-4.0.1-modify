package org.thingsboard.server.transport.tcp;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.AttributeKey;
import io.netty.util.ResourceLeakDetector;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.TbTransportService;

import java.net.InetSocketAddress;

/**
 * TCP Server服务
 * @author jiahaozz
 */
@Service("TcpTransportService")
@TbTcpTransportComponent
@Slf4j
public class TcpTransportService implements TbTransportService {

    public static AttributeKey<InetSocketAddress> ADDRESS = AttributeKey.newInstance("SRC_ADDRESS");

    @Value("${transport.tcp.bind_address}")
    private String host;
    @Value("${transport.tcp.bind_port}")
    private Integer port;


    @Value("${transport.tcp.netty.leak_detector_level}")
    private String leakDetectorLevel;
    @Value("${transport.tcp.netty.boss_group_thread_count}")
    private Integer bossGroupThreadCount;
    @Value("${transport.tcp.netty.worker_group_thread_count}")
    private Integer workerGroupThreadCount;
    @Value("${transport.tcp.netty.so_keep_alive}")
    private boolean keepAlive;

    @Autowired
    private TcpTransportContext context;

    private Channel serverChannel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    @PostConstruct
    public void init() throws Exception {
        log.info("Setting resource leak detector level to {}", leakDetectorLevel);
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.valueOf(leakDetectorLevel.toUpperCase()));

        log.info("Starting TCP transport...");
        bossGroup = new NioEventLoopGroup(bossGroupThreadCount);
        workerGroup = new NioEventLoopGroup(workerGroupThreadCount);
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new TcpTransportServerInitializer(context))
                .childOption(ChannelOption.SO_KEEPALIVE, keepAlive);

        serverChannel = b.bind(host, port).sync().channel();
        log.info("Tcp transport started!");
    }

    @PreDestroy
    public void shutdown() throws InterruptedException {
        log.info("Stopping TCP transport!");
        try {
            serverChannel.close().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
        log.info("TCP transport stopped!");
    }

    @Override
    public String getName() {
        return DataConstants.TCP_TRANSPORT_NAME;
    }
}
