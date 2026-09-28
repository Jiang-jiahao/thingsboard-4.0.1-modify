package com.jnks.iot.server.transport.tcp;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.device.profile.TcpTransportFramingMode;
import com.jnks.iot.server.transport.tcp.netty.TcpPipelineBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.Optional;


@RequiredArgsConstructor
@Slf4j
public class TcpTransportServerInitializer extends ChannelInitializer<SocketChannel> {
    private final TcpTransportContext tcpTransportContext;
    private final TcpTransportService tcpTransportService;
    @Override
    protected void initChannel(SocketChannel ch) {
        var session = tcpTransportContext.newInboundDeviceSession();
        TcpTransportFramingMode framingMode = tcpTransportService.getServerAuthFramingMode();
        int fixedLen = tcpTransportService.getServerAuthFixedFrameLength();
        // 档案声明的自定义监听端口只服务该档案：首帧（鉴权）即按档案的分帧解码，并提前绑定档案。
        // 共享默认端口上鉴权前无从判断档案，仍用全局 transport.tcp.server.auth_framing_mode，
        // 鉴权成功后由 afterSuccessfulAuth 替换为档案的分帧。
        InetSocketAddress localAddress = ch.localAddress();
        int localPort = localAddress != null ? localAddress.getPort() : -1;
        Optional<DeviceProfile> owner = tcpTransportContext.resolveInboundProfileForLocalPort(localPort);
        if (owner.isPresent()) {
            session.setDeviceProfile(owner.get());
            framingMode = session.getTcpTransportFramingMode();
            fixedLen = session.getTcpFixedFrameLengthForFraming();
            if (framingMode == TcpTransportFramingMode.FIXED_LENGTH && fixedLen <= 0) {
                log.warn("TCP profile [{}] uses FIXED_LENGTH but tcpFixedFrameLength is unset; "
                        + "falling back to the global auth fixed frame length on listen port {}", owner.get().getName(), localPort);
                fixedLen = tcpTransportService.getServerAuthFixedFrameLength();
                if (fixedLen <= 0) {
                    framingMode = TcpTransportFramingMode.LINE;
                    fixedLen = 0;
                }
            }
        }
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
