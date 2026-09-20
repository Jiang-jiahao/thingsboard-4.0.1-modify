package com.jnks.iot.server.transport.mqtt.util;

import lombok.Data;

@Data
public class AlwaysTrueTopicFilter implements MqttTopicFilter {

    @Override
    public boolean filter(String topic) {
        return true;
    }
}
