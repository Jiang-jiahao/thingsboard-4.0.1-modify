package org.thingsboard.server.transport.snmp.event;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.thingsboard.server.queue.discovery.TbApplicationEventListener;
import org.thingsboard.server.queue.discovery.event.ServiceListChangedEvent;
import org.thingsboard.server.transport.snmp.service.SnmpTransportBalancingService;

@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.snmp.enabled:false}'=='true'")
@Component
@RequiredArgsConstructor
public class ServiceListChangedEventListener extends TbApplicationEventListener<ServiceListChangedEvent> {
    private final SnmpTransportBalancingService snmpTransportBalancingService;

    @Override
    protected void onTbApplicationEvent(ServiceListChangedEvent event) {
        snmpTransportBalancingService.onServiceListChanged(event);
    }
}
