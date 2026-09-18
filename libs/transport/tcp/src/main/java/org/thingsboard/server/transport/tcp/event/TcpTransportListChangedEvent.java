package org.thingsboard.server.transport.tcp.event;
import org.thingsboard.server.queue.discovery.event.TbApplicationEvent;
public class TcpTransportListChangedEvent extends TbApplicationEvent {
    public TcpTransportListChangedEvent() {
        super(new Object());
    }
}