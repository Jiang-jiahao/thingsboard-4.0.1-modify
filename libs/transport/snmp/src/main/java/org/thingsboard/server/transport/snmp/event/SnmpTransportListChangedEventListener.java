package org.thingsboard.server.transport.snmp.event;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.thingsboard.server.queue.discovery.TbApplicationEventListener;
import org.thingsboard.server.transport.snmp.SnmpTransportContext;

@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.snmp.enabled:false}'=='true'")
@Component
@RequiredArgsConstructor
public class SnmpTransportListChangedEventListener extends TbApplicationEventListener<SnmpTransportListChangedEvent> {
    private final SnmpTransportContext snmpTransportContext;

    @Override
    protected void onTbApplicationEvent(SnmpTransportListChangedEvent event) {
        snmpTransportContext.onSnmpTransportListChanged();
    }
}
