package com.jnks.iot.server.transport.mqtt.event;

import com.jnks.iot.server.queue.discovery.event.JnksIotApplicationEvent;

public class MqttTransportListChangedEvent extends JnksIotApplicationEvent {
    public MqttTransportListChangedEvent() {
        super(new Object());
    }
}
