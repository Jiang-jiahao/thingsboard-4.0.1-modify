package com.jnks.iot.server.transport.snmp.event;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import com.jnks.iot.server.queue.discovery.JnksIotApplicationEventListener;
import com.jnks.iot.server.transport.snmp.SnmpTransportContext;

@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.snmp.enabled:false}'=='true'")
@Component
@RequiredArgsConstructor
public class SnmpTransportListChangedEventListener extends JnksIotApplicationEventListener<SnmpTransportListChangedEvent> {
    private final SnmpTransportContext snmpTransportContext;

    @Override
    protected void onJnksIotApplicationEvent(SnmpTransportListChangedEvent event) {
        snmpTransportContext.onSnmpTransportListChanged();
    }
}
