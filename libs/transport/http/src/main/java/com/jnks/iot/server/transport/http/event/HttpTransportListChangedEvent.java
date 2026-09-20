package com.jnks.iot.server.transport.http.event;

import com.jnks.iot.server.queue.discovery.event.TbApplicationEvent;

public class HttpTransportListChangedEvent extends TbApplicationEvent {
    public HttpTransportListChangedEvent() {
        super(new Object());
    }
}
