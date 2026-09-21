package com.jnks.iot.server.transport.http.event;

import com.jnks.iot.server.queue.discovery.event.JnksIotApplicationEvent;

public class HttpTransportListChangedEvent extends JnksIotApplicationEvent {
    public HttpTransportListChangedEvent() {
        super(new Object());
    }
}
