package com.jnks.iot.rule.engine.mqtt;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.rule.engine.credentials.AnonymousCredentials;
import com.jnks.iot.rule.engine.credentials.ClientCredentials;

@Data
public class JnksIotMqttNodeConfiguration implements NodeConfiguration<JnksIotMqttNodeConfiguration> {

    private String topicPattern;
    private String host;
    private int port;
    private int connectTimeoutSec;
    private String clientId;
    private boolean appendClientIdSuffix;
    private boolean retainedMessage;

    private boolean cleanSession;
    private boolean ssl;
    private boolean parseToPlainText;
    private ClientCredentials credentials;

    @Override
    public JnksIotMqttNodeConfiguration defaultConfiguration() {
        JnksIotMqttNodeConfiguration configuration = new JnksIotMqttNodeConfiguration();
        configuration.setTopicPattern("my-topic");
        configuration.setPort(1883);
        configuration.setConnectTimeoutSec(10);
        configuration.setCleanSession(true);
        configuration.setSsl(false);
        configuration.setRetainedMessage(false);
        configuration.setParseToPlainText(false);
        configuration.setCredentials(new AnonymousCredentials());
        return configuration;
    }

}
