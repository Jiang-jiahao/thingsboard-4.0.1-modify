package com.jnks.iot.rule.engine.mqtt.azure;

import io.netty.handler.codec.mqtt.MqttVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import com.jnks.iot.common.util.AzureIotHubUtil;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.mqtt.MqttClient;
import com.jnks.iot.mqtt.MqttClientConfig;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.credentials.CertPemCredentials;
import com.jnks.iot.rule.engine.mqtt.JnksIotMqttNodeConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.spy;
import static org.mockito.BDDMockito.willReturn;

@ExtendWith(MockitoExtension.class)
public class JnksIotAzureIotHubNodeTest {

    private JnksIotAzureIotHubNode azureIotHubNode;
    private JnksIotAzureIotHubNodeConfiguration azureIotHubNodeConfig;

    @Mock
    protected JnksIotContext ctxMock;
    @Mock
    protected MqttClient mqttClientMock;

    @BeforeEach
    public void setUp() {
        azureIotHubNode = spy(new JnksIotAzureIotHubNode());
        azureIotHubNodeConfig = new JnksIotAzureIotHubNodeConfiguration().defaultConfiguration();
    }

    @Test
    public void verifyDefaultConfig() {
        assertThat(azureIotHubNodeConfig.getTopicPattern()).isEqualTo("devices/<device_id>/messages/events/");
        assertThat(azureIotHubNodeConfig.getHost()).isEqualTo("<iot-hub-name>.azure-devices.net");
        assertThat(azureIotHubNodeConfig.getPort()).isEqualTo(8883);
        assertThat(azureIotHubNodeConfig.getConnectTimeoutSec()).isEqualTo(10);
        assertThat(azureIotHubNodeConfig.getClientId()).isNull();
        assertThat(azureIotHubNodeConfig.isAppendClientIdSuffix()).isFalse();
        assertThat(azureIotHubNodeConfig.isRetainedMessage()).isFalse();
        assertThat(azureIotHubNodeConfig.isCleanSession()).isTrue();
        assertThat(azureIotHubNodeConfig.isSsl()).isTrue();
        assertThat(azureIotHubNodeConfig.isParseToPlainText()).isFalse();
        assertThat(azureIotHubNodeConfig.getCredentials()).isInstanceOf(AzureIotHubSasCredentials.class);
    }

    @Test
    public void verifyPrepareMqttClientConfigMethodWithAzureIotHubSasCredentials() throws Exception {
        AzureIotHubSasCredentials credentials = new AzureIotHubSasCredentials();
        credentials.setSasKey("testSasKey");
        credentials.setCaCert("test-ca-cert.pem");
        azureIotHubNodeConfig.setCredentials(credentials);

        willReturn(mqttClientMock).given(azureIotHubNode).initAzureClient(any());
        azureIotHubNode.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(azureIotHubNodeConfig)));

        MqttClientConfig mqttClientConfig = new MqttClientConfig();
        azureIotHubNode.prepareMqttClientConfig(mqttClientConfig);

        assertThat(mqttClientConfig.getProtocolVersion()).isEqualTo(MqttVersion.MQTT_3_1_1);
        assertThat(mqttClientConfig.getUsername()).isEqualTo(AzureIotHubUtil.buildUsername(azureIotHubNodeConfig.getHost(), mqttClientConfig.getClientId()));
        assertThat(mqttClientConfig.getPassword()).isEqualTo(AzureIotHubUtil.buildSasToken(azureIotHubNodeConfig.getHost(), credentials.getSasKey()));
    }

    @Test
    public void givenPemCredentialsAndSuccessfulConnectResult_whenInit_thenOk() throws Exception {
        CertPemCredentials credentials = new CertPemCredentials();
        credentials.setCaCert("test-ca-cert.pem");
        credentials.setPassword("test-password");
        azureIotHubNodeConfig.setCredentials(credentials);

        willReturn(mqttClientMock).given(azureIotHubNode).initAzureClient(any());

        assertThatNoException().isThrownBy(
                () -> azureIotHubNode.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(azureIotHubNodeConfig))));

        var mqttNodeConfiguration = (JnksIotMqttNodeConfiguration) ReflectionTestUtils.getField(azureIotHubNode, "mqttNodeConfiguration");
        assertThat(mqttNodeConfiguration).isNotNull();
        assertThat(mqttNodeConfiguration.getPort()).isEqualTo(8883);
        assertThat(mqttNodeConfiguration.isCleanSession()).isTrue();
    }

}
