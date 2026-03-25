/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.transport.tcp;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.thingsboard.server.common.data.device.profile.TcpTransportFramingMode;
import java.net.InetSocketAddress;
import java.util.Optional;
import org.thingsboard.server.transport.tcp.netty.TcpPipelineBuilder;
import lombok.RequiredArgsConstructor;


@RequiredArgsConstructor
public class TcpTransportServerInitializer extends ChannelInitializer<SocketChannel> {
    private final TcpTransportContext tcpTransportContext;
    private final TcpTransportService tcpTransportService;
    @Override
    protected void initChannel(SocketChannel ch) {
        int localPort = ((InetSocketAddress) ch.localAddress()).getPort();
        var session = tcpTransportContext.newInboundDeviceSession();
        TcpTransportFramingMode framingMode = tcpTransportService.getServerAuthFramingMode();
        int fixedLen = tcpTransportService.getServerAuthFixedFrameLength();
        Optional<TcpInboundPipelineConfig> dedicatedCfg = tcpTransportContext.resolveInboundPipelineConfigForLocalPort(localPort);
        if (dedicatedCfg.isPresent()) {
            TcpInboundPipelineConfig cfg = dedicatedCfg.get();
            framingMode = cfg.getFramingMode();
            fixedLen = cfg.getFixedFrameLength();
            session.setDeviceProfile(cfg.getDeviceProfile());
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
