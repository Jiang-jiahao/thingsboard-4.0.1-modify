package com.jnks.iot.rule.engine.mqtt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.Promise;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.mqtt.MqttClient;
import com.jnks.iot.mqtt.MqttClientConfig;
import com.jnks.iot.mqtt.MqttConnectResult;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.credentials.BasicCredentials;
import com.jnks.iot.rule.engine.credentials.ClientCredentials;
import com.jnks.iot.rule.engine.credentials.CredentialsType;
import com.jnks.iot.rule.engine.external.JnksIotAbstractExternalNode;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.plugin.ComponentClusteringMode;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import javax.net.ssl.SSLException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@RuleNode(
        type = ComponentType.EXTERNAL,
        name = "mqtt",
        configClazz = JnksIotMqttNodeConfiguration.class,
        version = 1,
        clusteringMode = ComponentClusteringMode.USER_PREFERENCE,
        nodeDescription = "Publish messages to the MQTT broker",
        nodeDetails = "Will publish message payload to the MQTT broker with QoS <b>AT_LEAST_ONCE</b>.",
        configDirective = "jnksIotExternalNodeMqttConfig",
        icon = "call_split"
)
public class JnksIotMqttNode extends JnksIotAbstractExternalNode {

    private static final Charset UTF8 = StandardCharsets.UTF_8;

    private static final String ERROR = "error";

    protected JnksIotMqttNodeConfiguration mqttNodeConfiguration;

    protected MqttClient mqttClient;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        super.init(ctx);
        this.mqttNodeConfiguration = JnksIotNodeUtils.convert(configuration, JnksIotMqttNodeConfiguration.class);
        try {
            this.mqttClient = initClient(ctx);
        } catch (JnksIotNodeException e) {
            throw e;
        } catch (Exception e) {
            throw new JnksIotNodeException(e);
        }
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        String topic = JnksIotNodeUtils.processPattern(this.mqttNodeConfiguration.getTopicPattern(), msg);
        var jnksIotMsg = ackIfNeeded(ctx, msg);
        this.mqttClient.publish(topic, Unpooled.wrappedBuffer(getData(jnksIotMsg, mqttNodeConfiguration.isParseToPlainText()).getBytes(UTF8)),
                        MqttQoS.AT_LEAST_ONCE, mqttNodeConfiguration.isRetainedMessage())
                .addListener(future -> {
                            if (future.isSuccess()) {
                                tellSuccess(ctx, jnksIotMsg);
                            } else {
                                tellFailure(ctx, processException(jnksIotMsg, future.cause()), future.cause());
                            }
                        }
                );
    }

    private JnksIotMsg processException(JnksIotMsg origMsg, Throwable e) {
        JnksIotMsgMetaData metaData = origMsg.getMetaData().copy();
        metaData.putValue(ERROR, e.getClass() + ": " + e.getMessage());
        return origMsg.transform()
                .metaData(metaData)
                .build();
    }

    @Override
    public void destroy() {
        if (this.mqttClient != null) {
            this.mqttClient.disconnect();
        }
    }

    String getOwnerId(JnksIotContext ctx) {
        return "Tenant[" + ctx.getTenantId().getId() + "]RuleNode[" + ctx.getSelf().getId().getId() + "]";
    }

    protected MqttClient initClient(JnksIotContext ctx) throws Exception {
        MqttClientConfig config = new MqttClientConfig(getSslContext());
        config.setOwnerId(getOwnerId(ctx));
        if (!StringUtils.isEmpty(this.mqttNodeConfiguration.getClientId())) {
            config.setClientId(getClientId(ctx));
        }
        config.setCleanSession(this.mqttNodeConfiguration.isCleanSession());

        prepareMqttClientConfig(config);
        MqttClient client = getMqttClient(ctx, config);
        client.setEventLoop(ctx.getSharedEventLoop());
        Promise<MqttConnectResult> connectFuture = client.connect(this.mqttNodeConfiguration.getHost(), this.mqttNodeConfiguration.getPort());
        MqttConnectResult result;
        try {
            result = connectFuture.get(this.mqttNodeConfiguration.getConnectTimeoutSec(), TimeUnit.SECONDS);
        } catch (TimeoutException ex) {
            connectFuture.cancel(true);
            client.disconnect();
            String hostPort = this.mqttNodeConfiguration.getHost() + ":" + this.mqttNodeConfiguration.getPort();
            throw new RuntimeException(String.format("Failed to connect to MQTT broker at %s.", hostPort));
        }
        if (!result.isSuccess()) {
            connectFuture.cancel(true);
            client.disconnect();
            String hostPort = this.mqttNodeConfiguration.getHost() + ":" + this.mqttNodeConfiguration.getPort();
            throw new RuntimeException(String.format("Failed to connect to MQTT broker at %s. Result code is: %s", hostPort, result.getReturnCode()));
        }
        return client;
    }

    private String getClientId(JnksIotContext ctx) throws JnksIotNodeException {
        String clientId = this.mqttNodeConfiguration.isAppendClientIdSuffix() ?
                this.mqttNodeConfiguration.getClientId() + "_" + ctx.getServiceId() :
                this.mqttNodeConfiguration.getClientId();
        if (clientId.length() > 23) {
            throw new JnksIotNodeException("Client ID is too long '" + clientId + "'. " +
                    "The length of Client ID cannot be longer than 23, but current length is " + clientId.length() + ".", true);
        }
        return clientId;
    }

    MqttClient getMqttClient(JnksIotContext ctx, MqttClientConfig config) {
        return MqttClient.create(config, null, ctx.getExternalCallExecutor());
    }

    protected void prepareMqttClientConfig(MqttClientConfig config) {
        ClientCredentials credentials = this.mqttNodeConfiguration.getCredentials();
        if (credentials.getType() == CredentialsType.BASIC) {
            BasicCredentials basicCredentials = (BasicCredentials) credentials;
            config.setUsername(basicCredentials.getUsername());
            config.setPassword(basicCredentials.getPassword());
        }
    }

    private SslContext getSslContext() throws SSLException {
        return this.mqttNodeConfiguration.isSsl() ? this.mqttNodeConfiguration.getCredentials().initSslContext() : null;
    }

    private String getData(JnksIotMsg jnksIotMsg, boolean parseToPlainText) {
        if (parseToPlainText) {
            return JacksonUtil.toPlainText(jnksIotMsg.getData());
        }
        return jnksIotMsg.getData();
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        boolean hasChanges = false;
        switch (fromVersion) {
            case 0:
                String parseToPlainText = "parseToPlainText";
                if (!oldConfiguration.has(parseToPlainText)) {
                    hasChanges = true;
                    ((ObjectNode) oldConfiguration).put(parseToPlainText, false);
                }
                break;
            default:
                break;
        }
        return new JnksIotPair<>(hasChanges, oldConfiguration);
    }
}
