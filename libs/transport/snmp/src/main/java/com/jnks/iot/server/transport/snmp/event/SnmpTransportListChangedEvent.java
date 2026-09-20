package com.jnks.iot.server.transport.snmp.event;

import com.jnks.iot.server.queue.discovery.event.TbApplicationEvent;

public class SnmpTransportListChangedEvent extends TbApplicationEvent {
    public SnmpTransportListChangedEvent() {
        super(new Object());
    }
}
