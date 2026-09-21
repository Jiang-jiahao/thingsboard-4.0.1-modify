package com.jnks.iot.server.transport.snmp.event;

import com.jnks.iot.server.queue.discovery.event.JnksIotApplicationEvent;

public class SnmpTransportListChangedEvent extends JnksIotApplicationEvent {
    public SnmpTransportListChangedEvent() {
        super(new Object());
    }
}
