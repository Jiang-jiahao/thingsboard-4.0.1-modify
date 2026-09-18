package org.thingsboard.server.transport.udp.event;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.thingsboard.server.queue.discovery.TbApplicationEventListener;
import org.thingsboard.server.queue.discovery.event.ServiceListChangedEvent;
import org.thingsboard.server.transport.udp.UdpTransportBalancingService;
@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.udp.enabled:true}'=='true'")
@Component
@RequiredArgsConstructor
public class UdpServiceListChangedEventListener extends TbApplicationEventListener<ServiceListChangedEvent> {
    private final UdpTransportBalancingService tcpTransportBalancingService;
    @Override
    protected void onTbApplicationEvent(ServiceListChangedEvent event) {
        tcpTransportBalancingService.onServiceListChanged(event);
    }
}