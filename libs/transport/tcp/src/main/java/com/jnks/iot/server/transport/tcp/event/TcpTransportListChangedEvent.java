package com.jnks.iot.server.transport.tcp.event;
import com.jnks.iot.server.queue.discovery.event.TbApplicationEvent;
public class TcpTransportListChangedEvent extends TbApplicationEvent {
    public TcpTransportListChangedEvent() {
        super(new Object());
    }
}