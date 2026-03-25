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
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.transport.tcp.session.TcpDeviceSession;
import org.thingsboard.server.transport.tcp.util.TcpPayloadUtil;
import java.nio.charset.StandardCharsets;

@RequiredArgsConstructor
@Slf4j
public class TcpInboundHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private final TcpTransportContext tcpTransportContext;
    private final TcpDeviceSession session;
    /**
     * true：平台主动连接设备（CLIENT）；false：设备连入平台（SERVER）。
     */
    private final boolean outboundClient;
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        session.setChannel(ctx.channel());
        if (!outboundClient) {
            ctx.channel().config().setAutoRead(false);
            if (!tcpTransportContext.startServerWireAuth(ctx, session)) {
                ctx.channel().config().setAutoRead(true);
            }
            return;
        }
        if (session.shouldSendWireAuthPayload()) {
            String token = tcpTransportContext.getProtoEntityService().getDeviceCredentialsByDeviceId(session.getDeviceId()).getCredentialsId();
            session.sendAuthFrame(token);
        }
    }
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf buf) {
        byte[] data = new byte[buf.readableBytes()];
        buf.readBytes(data);
        try {
            if (!session.isCoreSessionReady()) {
                if (outboundClient) {
                    log.warn("[{}] Client session not ready on read", session.getSessionId());
                    return;
                }
                if (!session.tryBeginServerAuth()) {
                    ctx.close();
                    return;
                }
                String authJson = new String(data, StandardCharsets.UTF_8).trim();
                JsonObject root = JsonParser.parseString(authJson).getAsJsonObject();
                tcpTransportContext.getTcpMessageProcessor().processServerSideAuth(session, root,
                        msg -> tcpTransportContext.afterSuccessfulAuth(ctx, session, msg));
                return;
            }
            String jsonPayload = TcpPayloadUtil.decodePayloadBytes(session.getPayloadDataType(), data);
            session.processIncomingJsonLine(jsonPayload);
        } catch (Exception e) {
            log.warn("[{}] Bad TCP frame", session.getSessionId(), e);
            ctx.close();
        }
    }
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        tcpTransportContext.onChannelClosed(session);
    }
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("[{}] TCP exception", session.getSessionId(), cause);
        ctx.close();
    }
}