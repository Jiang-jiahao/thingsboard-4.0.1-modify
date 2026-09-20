package com.jnks.iot.server.transport.udp.event;
import com.jnks.iot.server.queue.discovery.event.TbApplicationEvent;
public class UdpTransportListChangedEvent extends TbApplicationEvent {
    public UdpTransportListChangedEvent() {
        super(new Object());
    }
}