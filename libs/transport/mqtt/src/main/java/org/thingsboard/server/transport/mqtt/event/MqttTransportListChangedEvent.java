package org.thingsboard.server.transport.mqtt.event;

import org.thingsboard.server.queue.discovery.event.TbApplicationEvent;

public class MqttTransportListChangedEvent extends TbApplicationEvent {
    public MqttTransportListChangedEvent() {
        super(new Object());
    }
}
