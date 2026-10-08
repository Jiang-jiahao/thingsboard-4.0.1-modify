package com.jnks.iot.server.transport.udp.event;
import com.jnks.iot.server.queue.discovery.event.JnksIotApplicationEvent;


public class UdpTransportListChangedEvent extends JnksIotApplicationEvent {

    public UdpTransportListChangedEvent() {
        super(new Object());
    }
}