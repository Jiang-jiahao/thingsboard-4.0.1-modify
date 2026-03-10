package org.thingsboard.server.transport.tcp;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.thingsboard.server.common.transport.TransportContext;
import org.thingsboard.server.common.transport.TransportTenantProfileCache;


import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TCP Server传输上下文
 * @author jiahaozz
 */
@Slf4j
@Component
@TbTcpTransportComponent
public class TcpTransportContext extends TransportContext {


    @Getter
    @Autowired
    private TransportTenantProfileCache tenantProfileCache;

    @Getter
    @Value("${transport.tcp.proxy_enabled:false}")
    private boolean proxyEnabled;



    private final AtomicInteger connectionsCounter = new AtomicInteger();

    @PostConstruct
    public void init() {
        super.init();
        transportService.createGaugeStats("openConnections", connectionsCounter);
    }

    public void channelRegistered() {
        connectionsCounter.incrementAndGet();
    }

    public void channelUnregistered() {
        connectionsCounter.decrementAndGet();
    }

    public boolean checkAddress(InetSocketAddress address) {
        return rateLimitService.checkAddress(address);
    }

    public void onAuthSuccess(InetSocketAddress address) {
        rateLimitService.onAuthSuccess(address);
    }

    public void onAuthFailure(InetSocketAddress address) {
        rateLimitService.onAuthFailure(address);
    }

}
