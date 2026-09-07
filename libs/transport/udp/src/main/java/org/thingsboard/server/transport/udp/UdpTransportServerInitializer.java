/**
 * Copyright © 2016-2025 The Thingsboard Authors
 */
package org.thingsboard.server.transport.udp;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.DatagramChannel;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class UdpTransportServerInitializer extends ChannelInitializer<DatagramChannel> {

    private final UdpTransportContext udpTransportContext;
    private final UdpTransportService udpTransportService;

    @Override
    protected void initChannel(DatagramChannel ch) {
        ch.pipeline().addLast(new UdpInboundHandler(udpTransportContext, udpTransportService));
    }
}
