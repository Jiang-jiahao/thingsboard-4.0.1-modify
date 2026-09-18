package org.thingsboard.server.transport.tcp.event;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.thingsboard.server.queue.discovery.TbApplicationEventListener;
import org.thingsboard.server.queue.discovery.event.ServiceListChangedEvent;
import org.thingsboard.server.transport.tcp.TcpTransportBalancingService;
@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.tcp.enabled:true}'=='true'")
@Component
@RequiredArgsConstructor
public class TcpServiceListChangedEventListener extends TbApplicationEventListener<ServiceListChangedEvent> {
    private final TcpTransportBalancingService tcpTransportBalancingService;
    @Override
    protected void onTbApplicationEvent(ServiceListChangedEvent event) {
        tcpTransportBalancingService.onServiceListChanged(event);
    }
}