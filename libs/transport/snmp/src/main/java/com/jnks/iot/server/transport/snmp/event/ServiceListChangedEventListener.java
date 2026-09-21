package com.jnks.iot.server.transport.snmp.event;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import com.jnks.iot.server.queue.discovery.JnksIotApplicationEventListener;
import com.jnks.iot.server.queue.discovery.event.ServiceListChangedEvent;
import com.jnks.iot.server.transport.snmp.service.SnmpTransportBalancingService;

@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.snmp.enabled:false}'=='true'")
@Component
@RequiredArgsConstructor
public class ServiceListChangedEventListener extends JnksIotApplicationEventListener<ServiceListChangedEvent> {
    private final SnmpTransportBalancingService snmpTransportBalancingService;

    @Override
    protected void onJnksIotApplicationEvent(ServiceListChangedEvent event) {
        snmpTransportBalancingService.onServiceListChanged(event);
    }
}
