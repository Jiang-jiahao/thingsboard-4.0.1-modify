package com.jnks.iot.rule.engine.mqtt;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.mqtt.MqttClient;
import com.jnks.iot.mqtt.MqttClientConfig;
import com.jnks.iot.mqtt.MqttConnectResult;
import com.jnks.iot.rule.engine.AbstractRuleNodeUpgradeTest;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.credentials.AnonymousCredentials;
import com.jnks.iot.rule.engine.credentials.BasicCredentials;
import com.jnks.iot.rule.engine.credentials.CertPemCredentials;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.RuleNodeId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.data.rule.RuleNode;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.mock;
import static org.mockito.BDDMockito.never;
import static org.mockito.BDDMockito.spy;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;

@ExtendWith(MockitoExtension.class)
public class JnksIotMqttNodeTest extends AbstractRuleNodeUpgradeTest {

    private final TenantId TENANT_ID = TenantId.fromUUID(UUID.fromString("d0c5d2a8-3a6e-4c95-8caf-47fbdc8ef98f"));
    private final DeviceId DEVICE_ID = new DeviceId(UUID.fromString("09115d92-d333-432a-868c-ccd6e89c9287"));
    private final RuleNodeId RULE_NODE_ID = new RuleNodeId(UUID.fromString("11699e8f-c3f0-4366-9334-cbf75798314b"));

    protected JnksIotMqttNode mqttNode;
    protected JnksIotMqttNodeConfiguration mqttNodeConfig;

    @Mock
    protected JnksIotContext ctxMock;
    @Mock
    protected MqttClient mqttClientMock;
    @Mock
    protected EventLoopGroup eventLoopGroupMock;
    @Mock
    protected Promise<MqttConnectResult> promiseMock;
    @Mock
    protected MqttConnectResult resultMock;

    @BeforeEach
    protected void setUp() {
        mqttNode = spy(new JnksIotMqttNode());
        mqttNodeConfig = new JnksIotMqttNodeConfiguration().defaultConfiguration();
    }

    @Test
    public void verifyDefaultConfig() {
        assertThat(mqttNodeConfig.getTopicPattern()).isEqualTo("my-topic");
        assertThat(mqttNodeConfig.getHost()).isNull();
        assertThat(mqttNodeConfig.getPort()).isEqualTo(1883);
        assertThat(mqttNodeConfig.getConnectTimeoutSec()).isEqualTo(10);
        assertThat(mqttNodeConfig.getClientId()).isNull();
        assertThat(mqttNodeConfig.isAppendClientIdSuffix()).isFalse();
        assertThat(mqttNodeConfig.isRetainedMessage()).isFalse();
        assertThat(mqttNodeConfig.isCleanSession()).isTrue();
        assertThat(mqttNodeConfig.isSsl()).isFalse();
        assertThat(mqttNodeConfig.isParseToPlainText()).isFalse();
        assertThat(mqttNodeConfig.getCredentials()).isInstanceOf(AnonymousCredentials.class);
    }

    @Test
    public void verifyGetOwnerIdMethod() {
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);
        given(ctxMock.getSelf()).willReturn(new RuleNode(RULE_NODE_ID));

        String actualOwnerIdStr = mqttNode.getOwnerId(ctxMock);
        String expectedOwnerIdStr = "Tenant[" + TENANT_ID.getId() + "]RuleNode[" + RULE_NODE_ID.getId() + "]";
        assertThat(actualOwnerIdStr).isEqualTo(expectedOwnerIdStr);
    }

    @Test
    public void verifyPrepareMqttClientConfigMethodWithBasicCredentials() throws Exception {
        BasicCredentials credentials = new BasicCredentials();
        credentials.setUsername("test_username");
        credentials.setPassword("test_password");
        mqttNodeConfig.setCredentials(credentials);

        mockSuccessfulInit();
        mqttNode.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(mqttNodeConfig)));

        MqttClientConfig mqttClientConfig = new MqttClientConfig();
        mqttNode.prepareMqttClientConfig(mqttClientConfig);

        assertThat(mqttClientConfig.getUsername()).isEqualTo("test_username");
        assertThat(mqttClientConfig.getPassword()).isEqualTo("test_password");
    }

    @Test
    public void givenSslIsTrueAndCredentials_whenGetSslContext_thenVerifySslContext() throws Exception {
        mqttNodeConfig.setSsl(true);
        mqttNodeConfig.setCredentials(new BasicCredentials());

        mockSuccessfulInit();
        mqttNode.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(mqttNodeConfig)));

        ArgumentCaptor<MqttClientConfig> mqttClientConfig = ArgumentCaptor.forClass(MqttClientConfig.class);
        then(mqttNode).should().prepareMqttClientConfig(mqttClientConfig.capture());
        SslContext actualSslContext = mqttClientConfig.getValue().getSslContext();
        assertThat(actualSslContext)
                .usingRecursiveComparison()
                .ignoringFields("ctx", "ctxLock", "sessionContext.context.ctx", "sessionContext.context.ctxLock")
                .isEqualTo(SslContextBuilder.forClient().build());
    }

    @Test
    public void givenSslIsFalse_whenGetSslContext_thenVerifySslContextIsNull() throws Exception {
        mqttNodeConfig.setSsl(false);

        mockSuccessfulInit();
        mqttNode.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(mqttNodeConfig)));

        ArgumentCaptor<MqttClientConfig> mqttClientConfig = ArgumentCaptor.forClass(MqttClientConfig.class);
        then(mqttNode).should().prepareMqttClientConfig(mqttClientConfig.capture());
        SslContext actualSslContext = mqttClientConfig.getValue().getSslContext();
        assertThat(actualSslContext).isNull();
    }

    @Test
    public void givenSuccessfulConnectResult_whenInit_thenOk() throws Exception {
        mqttNodeConfig.setClientId("bfrbTESTmfkr23");
        mqttNodeConfig.setAppendClientIdSuffix(true);
        mqttNodeConfig.setCredentials(new CertPemCredentials());

        mockSuccessfulInit();

        assertThatNoException().isThrownBy(() -> mqttNode.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(mqttNodeConfig))));
    }

    @Test
    public void givenClientIdIsTooLong_whenInit_thenThrowsException() {
        String invalidClientId = "vhfrbeb38ygwfwrgfwefgterhytjytj";
        mqttNodeConfig.setClientId(invalidClientId);

        given(ctxMock.getTenantId()).willReturn(TENANT_ID);
        given(ctxMock.getSelf()).willReturn(new RuleNode(RULE_NODE_ID));

        assertThatThrownBy(() -> mqttNode.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(mqttNodeConfig))))
                .isInstanceOf(JnksIotNodeException.class)
                .hasMessage("Client ID is too long '" + invalidClientId + "'. " +
                        "The length of Client ID cannot be longer than 23, but current length is " + invalidClientId.length() + ".")
                .extracting(e -> ((JnksIotNodeException) e).isUnrecoverable())
                .isEqualTo(true);
    }

    @Test
    public void givenClientIdIsOkAndAppendClientIdSuffixIsTrue_whenInit_thenClientIdBecomesInvalidAndThrowsException() {
        String validClientId = "fertjnhnjj4ge";
        mqttNodeConfig.setClientId("fertjnhnjj4ge");
        mqttNodeConfig.setAppendClientIdSuffix(true);

        given(ctxMock.getTenantId()).willReturn(TENANT_ID);
        given(ctxMock.getSelf()).willReturn(new RuleNode(RULE_NODE_ID));
        String serviceId = "test-service";
        given(ctxMock.getServiceId()).willReturn(serviceId);

        String resultedClientId = validClientId + "_" + serviceId;
        assertThatThrownBy(() -> mqttNode.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(mqttNodeConfig))))
                .isInstanceOf(JnksIotNodeException.class)
                .hasMessage("Client ID is too long '" + resultedClientId + "'. " +
                        "The length of Client ID cannot be longer than 23, but current length is " + resultedClientId.length() + ".")
                .extracting(e -> ((JnksIotNodeException) e).isUnrecoverable())
                .isEqualTo(true);
    }

    @Test
    public void givenFailedByTimeoutConnectResult_whenInit_thenThrowsException() throws ExecutionException, InterruptedException, TimeoutException {
        mqttNodeConfig.setHost("localhost");
        mqttNodeConfig.setClientId("bfrbTESTmfkr23");
        mqttNodeConfig.setCredentials(new CertPemCredentials());

        mockConnectClient();
        given(promiseMock.get(anyLong(), any(TimeUnit.class))).willThrow(new TimeoutException("Failed to connect"));

        assertThatThrownBy(() -> mqttNode.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(mqttNodeConfig))))
                .isInstanceOf(JnksIotNodeException.class)
                .hasMessage("java.lang.RuntimeException: Failed to connect to MQTT broker at localhost:1883.")
                .extracting(e -> ((JnksIotNodeException) e).isUnrecoverable())
                .isEqualTo(false);
    }

    @Test
    public void givenFailedConnectResult_whenInit_thenThrowsException() throws Exception {
        mqttNodeConfig.setHost("localhost");
        mqttNodeConfig.setClientId("bfrbTESTmfkr23");
        mqttNodeConfig.setAppendClientIdSuffix(true);
        mqttNodeConfig.setCredentials(new CertPemCredentials());

        mockConnectClient();
        given(promiseMock.get(anyLong(), any(TimeUnit.class))).willReturn(resultMock);
        given(resultMock.isSuccess()).willReturn(false);
        given(resultMock.getReturnCode()).willReturn(MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED);

        assertThatThrownBy(() -> mqttNode.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(mqttNodeConfig))))
                .isInstanceOf(JnksIotNodeException.class)
                .hasMessage("java.lang.RuntimeException: Failed to connect to MQTT broker at localhost:1883. Result code is: CONNECTION_REFUSED_NOT_AUTHORIZED")
                .extracting(e -> ((JnksIotNodeException) e).isUnrecoverable())
                .isEqualTo(false);
    }

    @ParameterizedTest
    @MethodSource
    public void givenForceAckIsTrueAndTopicPatternAndIsRetainedMsgIsTrue_whenOnMsg_thenTellSuccess(
            String topicPattern, JnksIotMsgMetaData metaData, String data
    ) throws Exception {
        mqttNodeConfig.setRetainedMessage(true);
        mqttNodeConfig.setTopicPattern(topicPattern);

        given(ctxMock.isExternalNodeForceAck()).willReturn(true);
        mockSuccessfulInit();
        mqttNode.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(mqttNodeConfig)));

        Future<Void> future = mock(Future.class);
        given(future.isSuccess()).willReturn(true);
        given(mqttClientMock.publish(any(String.class), any(ByteBuf.class), any(MqttQoS.class), anyBoolean())).willReturn(future);
        willAnswer(invocation -> {
            GenericFutureListener<Future<Void>> listener = invocation.getArgument(0);
            listener.operationComplete(future);
            return null;
        }).given(future).addListener(any());

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(metaData)
                .data(data)
                .build();
        mqttNode.onMsg(ctxMock, msg);

        then(ctxMock).should().ack(msg);
        String expectedTopic = JnksIotNodeUtils.processPattern(mqttNodeConfig.getTopicPattern(), msg);
        then(mqttClientMock).should().publish(expectedTopic, Unpooled.wrappedBuffer(msg.getData().getBytes(StandardCharsets.UTF_8)), MqttQoS.AT_LEAST_ONCE, true);
        ArgumentCaptor<JnksIotMsg> actualMsg = ArgumentCaptor.forClass(JnksIotMsg.class);
        then(ctxMock).should().enqueueForTellNext(actualMsg.capture(), eq(JnksIotNodeConnectionType.SUCCESS));
        assertThat(actualMsg.getValue()).usingRecursiveComparison().ignoringFields("ctx").isEqualTo(msg);
    }

    private static Stream<Arguments> givenForceAckIsTrueAndTopicPatternAndIsRetainedMsgIsTrue_whenOnMsg_thenTellSuccess() {
        return Stream.of(
                Arguments.of("new-topic", JnksIotMsgMetaData.EMPTY, JnksIotMsg.EMPTY_JSON_OBJECT),
                Arguments.of("${md-topic-name}", new JnksIotMsgMetaData(Map.of("md-topic-name", "md-new-topic")), JnksIotMsg.EMPTY_JSON_OBJECT),
                Arguments.of("$[msg-topic-name]", JnksIotMsgMetaData.EMPTY, "{\"msg-topic-name\":\"msg-new-topic\"}")
        );
    }

    @Test
    public void givenForceAckIsFalseParseToPlainTextIsTrueAndMsgPublishingFailed_whenOnMsg_thenTellFailure() throws Exception {
        mqttNodeConfig.setParseToPlainText(true);

        given(ctxMock.isExternalNodeForceAck()).willReturn(false);
        mockSuccessfulInit();
        mqttNode.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(mqttNodeConfig)));

        Future<Void> future = mock(Future.class);
        given(mqttClientMock.publish(any(String.class), any(ByteBuf.class), any(MqttQoS.class), anyBoolean())).willReturn(future);
        given(future.isSuccess()).willReturn(false);
        String errorMsg = "Message publishing was failed!";
        Throwable exception = new RuntimeException(errorMsg);
        given(future.cause()).willReturn(exception);
        willAnswer(invocation -> {
            GenericFutureListener<Future<Void>> listener = invocation.getArgument(0);
            listener.operationComplete(future);
            return null;
        }).given(future).addListener(any());

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data("\"string\"")
                .build();
        mqttNode.onMsg(ctxMock, msg);

        then(ctxMock).should(never()).ack(msg);
        String expectedData = JacksonUtil.toPlainText(msg.getData());
        then(mqttClientMock).should().publish(mqttNodeConfig.getTopicPattern(), Unpooled.wrappedBuffer(expectedData.getBytes(StandardCharsets.UTF_8)), MqttQoS.AT_LEAST_ONCE, false);
        JnksIotMsgMetaData metaData = new JnksIotMsgMetaData();
        metaData.putValue("error", RuntimeException.class + ": " + errorMsg);
        JnksIotMsg expectedMsg = msg.transform()
                .metaData(metaData)
                .build();
        ArgumentCaptor<JnksIotMsg> actualMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        then(ctxMock).should().tellFailure(actualMsgCaptor.capture(), eq(exception));
        JnksIotMsg actualMsg = actualMsgCaptor.getValue();
        assertThat(actualMsg).usingRecursiveComparison().ignoringFields("ctx").isEqualTo(expectedMsg);
    }

    @Test
    public void givenMqttClientIsNotNull_whenDestroy_thenDisconnect() {
        ReflectionTestUtils.setField(mqttNode, "mqttClient", mqttClientMock);
        mqttNode.destroy();
        then(mqttClientMock).should().disconnect();
    }

    @Test
    public void givenMqttClientIsNull_whenDestroy_thenShouldHaveNoInteractions() {
        ReflectionTestUtils.setField(mqttNode, "mqttClient", null);
        mqttNode.destroy();
        then(mqttClientMock).shouldHaveNoInteractions();
    }

    private static Stream<Arguments> givenFromVersionAndConfig_whenUpgrade_thenVerifyHasChangesAndConfig() {
        return Stream.of(
                // default config for version 0
                Arguments.of(0,
                        "{\"topicPattern\":\"my-topic\",\"port\":1883,\"connectTimeoutSec\":10,\"cleanSession\":true, \"ssl\":false, \"retainedMessage\":false,\"credentials\":{\"type\":\"anonymous\"}}",
                        true,
                        "{\"topicPattern\":\"my-topic\",\"port\":1883,\"connectTimeoutSec\":10,\"cleanSession\":true, \"ssl\":false, \"retainedMessage\":false,\"credentials\":{\"type\":\"anonymous\"},\"parseToPlainText\":false}"),
                // default config for version 1 with upgrade from version 0
                Arguments.of(1,
                        "{\"topicPattern\":\"my-topic\",\"port\":1883,\"connectTimeoutSec\":10,\"cleanSession\":true, \"ssl\":false, \"retainedMessage\":false,\"credentials\":{\"type\":\"anonymous\"},\"parseToPlainText\":false}",
                        false,
                        "{\"topicPattern\":\"my-topic\",\"port\":1883,\"connectTimeoutSec\":10,\"cleanSession\":true, \"ssl\":false, \"retainedMessage\":false,\"credentials\":{\"type\":\"anonymous\"},\"parseToPlainText\":false}")
        );

    }

    @Override
    protected JnksIotNode getTestNode() {
        return mqttNode;
    }

    private void mockConnectClient() {
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);
        given(ctxMock.getSelf()).willReturn(new RuleNode(RULE_NODE_ID));
        given(ctxMock.getSharedEventLoop()).willReturn(eventLoopGroupMock);
        willReturn(mqttClientMock).given(mqttNode).getMqttClient(any(), any());
        given(mqttClientMock.connect(any(), anyInt())).willReturn(promiseMock);
    }

    private void mockSuccessfulInit() throws Exception {
        mockConnectClient();
        given(promiseMock.get(anyLong(), any(TimeUnit.class))).willReturn(resultMock);
        given(resultMock.isSuccess()).willReturn(true);
    }

}
