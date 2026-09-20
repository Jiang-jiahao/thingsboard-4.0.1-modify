package com.jnks.iot.server.transport.mqtt.util;

public interface MqttTopicFilter {

    boolean filter(String topic);

}
