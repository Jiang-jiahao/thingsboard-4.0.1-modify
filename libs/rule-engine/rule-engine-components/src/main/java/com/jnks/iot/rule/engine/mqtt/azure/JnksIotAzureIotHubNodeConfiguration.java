package com.jnks.iot.rule.engine.mqtt.azure;

import lombok.Data;
import com.jnks.iot.rule.engine.mqtt.JnksIotMqttNodeConfiguration;

@Data
public class JnksIotAzureIotHubNodeConfiguration extends JnksIotMqttNodeConfiguration {

    @Override
    public JnksIotAzureIotHubNodeConfiguration defaultConfiguration() {
        JnksIotAzureIotHubNodeConfiguration configuration = new JnksIotAzureIotHubNodeConfiguration();
        configuration.setTopicPattern("devices/<device_id>/messages/events/");
        configuration.setHost("<iot-hub-name>.azure-devices.net");
        configuration.setPort(8883);
        configuration.setConnectTimeoutSec(10);
        configuration.setCleanSession(true);
        configuration.setSsl(true);
        configuration.setCredentials(new AzureIotHubSasCredentials());
        return configuration;
    }

}
