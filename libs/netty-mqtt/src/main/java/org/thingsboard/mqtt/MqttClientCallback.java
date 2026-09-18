package org.thingsboard.mqtt;

import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;

/**
 * Created by Valerii Sosliuk on 12/30/2017.
 */
public interface MqttClientCallback {

    /**
     * This method is called when the connection to the server is lost.
     *
     * @param cause the reason behind the loss of connection.
     */
    void connectionLost(Throwable cause);

    /**
     * This method is called when the connection to the server is recovered.
     *
     */
    void onSuccessfulReconnect();

    default void onConnAck(MqttConnAckMessage connAckMessage) {
    }

    default void onPubAck(MqttPubAckMessage pubAckMessage) {
    }

    default void onSubAck(MqttSubAckMessage pubAckMessage) {
    }

    default void onUnsubAck(MqttUnsubAckMessage unsubAckMessage) {
    }

    default void onDisconnect(MqttMessage mqttDisconnectMessage) {
    }
}
