package org.thingsboard.server.transport.http.event;

import org.thingsboard.server.queue.discovery.event.TbApplicationEvent;

public class HttpTransportListChangedEvent extends TbApplicationEvent {
    public HttpTransportListChangedEvent() {
        super(new Object());
    }
}
