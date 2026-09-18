package org.thingsboard.server.transport.tcp;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.thingsboard.server.common.data.device.profile.TcpTransportFramingMode;
import org.thingsboard.server.transport.tcp.netty.TcpPipelineBuilder;
import lombok.RequiredArgsConstructor;


@RequiredArgsConstructor
public class TcpTransportServerInitializer extends ChannelInitializer<SocketChannel> {
    private final TcpTransportContext tcpTransportContext;
    private final TcpTransportService tcpTransportService;
    @Override
    protected void initChannel(SocketChannel ch) {
        var session = tcpTransportContext.newInboundDeviceSession();
        // 共享监听端口：首帧（鉴权）统一用全局 transport.tcp.server.auth_framing_mode 分帧，
        // 鉴权成功后由 afterSuccessfulAuth 按设备档案替换（见 TcpTransportContext）。
        TcpTransportFramingMode framingMode = tcpTransportService.getServerAuthFramingMode();
        int fixedLen = tcpTransportService.getServerAuthFixedFrameLength();
        session.setInboundPipelineFramingMode(framingMode);
        session.setInboundPipelineFixedFrameLength(fixedLen);
        TcpPipelineBuilder.addFramingFirst(ch.pipeline(),
                framingMode,
                tcpTransportService.getMaxFrameLength(),
                fixedLen);
        ch.pipeline().addLast(TcpPipelineBuilder.INBOUND_HANDLER_NAME,
                new TcpInboundHandler(tcpTransportContext, session, false));
    }
}
