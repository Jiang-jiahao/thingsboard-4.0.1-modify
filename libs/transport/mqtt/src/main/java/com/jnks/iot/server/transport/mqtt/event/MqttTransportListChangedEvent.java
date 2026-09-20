package com.jnks.iot.server.transport.mqtt.event;

import com.jnks.iot.server.queue.discovery.event.TbApplicationEvent;

public class MqttTransportListChangedEvent extends TbApplicationEvent {
    public MqttTransportListChangedEvent() {
        super(new Object());
    }
}
