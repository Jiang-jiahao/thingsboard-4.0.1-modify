package com.jnks.iot.server.transport.udp.event;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import com.jnks.iot.server.queue.discovery.JnksIotApplicationEventListener;
import com.jnks.iot.server.queue.discovery.event.ServiceListChangedEvent;
import com.jnks.iot.server.transport.udp.UdpTransportBalancingService;
@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.udp.enabled:true}'=='true'")
@Component
@RequiredArgsConstructor
public class UdpServiceListChangedEventListener extends JnksIotApplicationEventListener<ServiceListChangedEvent> {
    private final UdpTransportBalancingService tcpTransportBalancingService;
    @Override
    protected void onJnksIotApplicationEvent(ServiceListChangedEvent event) {
        tcpTransportBalancingService.onServiceListChanged(event);
    }
}