package com.jnks.iot.server.transport.tcp.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TcpProxyProtocolHandlerTest {

    @Test
    void proxyHeaderReplacesSourceAddressAndResolvesBeforeAnyFrame() {
        AtomicBoolean resolved = new AtomicBoolean();
        AtomicReference<SocketAddress> addressAtResolve = new AtomicReference<>();
        EmbeddedChannel channel = new EmbeddedChannel(
                new HAProxyMessageDecoder(),
                new TcpProxyProtocolHandler(),
                new ChannelInboundHandlerAdapter() {
                    @Override
                    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                        if (evt instanceof TcpProxyProtocolHandler.AddressResolved) {
                            addressAtResolve.set(TcpProxyProtocolHandler.resolve(ctx.channel()));
                            resolved.set(true);
                        }
                    }
                });

        channel.writeInbound(Unpooled.copiedBuffer(
                "PROXY TCP4 10.0.0.5 172.18.0.2 4242 1026\r\nhello", StandardCharsets.US_ASCII));

        assertTrue(resolved.get(), "AddressResolved must fire before any business frame");
        assertEquals(new InetSocketAddress("10.0.0.5", 4242), addressAtResolve.get());
        assertEquals("hello", payload(channel));
        assertNull(channel.pipeline().get(TcpProxyProtocolHandler.class),
                "handler removes itself once the header is consumed");
        channel.finishAndReleaseAll();
    }

    @Test
    void resolveFallsBackToChannelAddressWhenNoHeaderSeen() {
        EmbeddedChannel channel = new EmbeddedChannel(new TcpProxyProtocolHandler());
        assertEquals(channel.remoteAddress(), TcpProxyProtocolHandler.resolve(channel));
        channel.finishAndReleaseAll();
    }

    @Test
    void connectionWithoutHeaderFailsFast() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new HAProxyMessageDecoder(),
                new TcpProxyProtocolHandler(),
                new ChannelInboundHandlerAdapter() {
                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                        ctx.close();
                    }
                });
        channel.writeInbound(Unpooled.copiedBuffer("GET / HTTP/1.1\r\n", StandardCharsets.US_ASCII));
        assertNull(channel.attr(TcpProxyProtocolHandler.REAL_REMOTE_ADDRESS).get(),
                "no address may be resolved from a non-PROXY stream");
        assertFalse(channel.isOpen());
        channel.finishAndReleaseAll();
    }

    private static String payload(EmbeddedChannel channel) {
        Object inbound = channel.readInbound();
        if (!(inbound instanceof ByteBuf buf)) {
            return null;
        }
        String value = buf.toString(StandardCharsets.US_ASCII);
        buf.release();
        return value;
    }
}
