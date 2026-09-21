package com.jnks.iot.rule.engine.mqtt.azure;

import io.netty.handler.codec.mqtt.MqttVersion;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.AzureIotHubUtil;
import com.jnks.iot.mqtt.MqttClient;
import com.jnks.iot.mqtt.MqttClientConfig;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.credentials.CertPemCredentials;
import com.jnks.iot.rule.engine.credentials.ClientCredentials;
import com.jnks.iot.rule.engine.credentials.CredentialsType;
import com.jnks.iot.rule.engine.mqtt.JnksIotMqttNode;
import com.jnks.iot.rule.engine.mqtt.JnksIotMqttNodeConfiguration;
import com.jnks.iot.server.common.data.plugin.ComponentClusteringMode;
import com.jnks.iot.server.common.data.plugin.ComponentType;

@Slf4j
@RuleNode(
        type = ComponentType.EXTERNAL,
        name = "azure iot hub",
        configClazz = JnksIotAzureIotHubNodeConfiguration.class,
        clusteringMode = ComponentClusteringMode.SINGLETON,
        nodeDescription = "Publish messages to the Azure IoT Hub",
        nodeDetails = "Will publish message payload to the Azure IoT Hub with QoS <b>AT_LEAST_ONCE</b>.",
        configDirective = "jnksIotExternalNodeAzureIotHubConfig"
)
public class JnksIotAzureIotHubNode extends JnksIotMqttNode {
    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        super.init(ctx);
        this.mqttNodeConfiguration = JnksIotNodeUtils.convert(configuration, JnksIotMqttNodeConfiguration.class);
        try {
            mqttNodeConfiguration.setPort(8883);
            mqttNodeConfiguration.setCleanSession(true);
            ClientCredentials credentials = mqttNodeConfiguration.getCredentials();
            if (CredentialsType.CERT_PEM == credentials.getType()) {
                CertPemCredentials pemCredentials = (CertPemCredentials) credentials;
                if (pemCredentials.getCaCert() == null || pemCredentials.getCaCert().isEmpty()) {
                    pemCredentials.setCaCert(AzureIotHubUtil.getDefaultCaCert());
                }
            }
            this.mqttClient = initAzureClient(ctx);
        } catch (Exception e) {
            throw new JnksIotNodeException(e);
        }
    }

    protected void prepareMqttClientConfig(MqttClientConfig config) {
        config.setProtocolVersion(MqttVersion.MQTT_3_1_1);
        config.setUsername(AzureIotHubUtil.buildUsername(mqttNodeConfiguration.getHost(), config.getClientId()));
        ClientCredentials credentials = mqttNodeConfiguration.getCredentials();
        if (CredentialsType.SAS == credentials.getType()) {
            config.setPassword(AzureIotHubUtil.buildSasToken(mqttNodeConfiguration.getHost(), ((AzureIotHubSasCredentials) credentials).getSasKey()));
        }
    }

    MqttClient initAzureClient(JnksIotContext ctx) throws Exception {
        return initClient(ctx);
    }
}
