package org.thingsboard.server.transport.udp.event;
import org.thingsboard.server.queue.discovery.event.TbApplicationEvent;
public class UdpTransportListChangedEvent extends TbApplicationEvent {
    public UdpTransportListChangedEvent() {
        super(new Object());
    }
}