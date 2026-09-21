package com.jnks.iot.server.transport.tcp.event;
import com.jnks.iot.server.queue.discovery.event.JnksIotApplicationEvent;
public class TcpTransportListChangedEvent extends JnksIotApplicationEvent {
    public TcpTransportListChangedEvent() {
        super(new Object());
    }
}