package com.jnks.iot.rule.engine.rocketmq;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.TestDbCallbackExecutor;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.RuleNodeId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.never;
import static org.mockito.BDDMockito.spy;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.BDDMockito.willThrow;

@ExtendWith(MockitoExtension.class)
public class JnksIotRocketMqNodeTest {

    private static final DeviceId DEVICE_ID = new DeviceId(UUID.fromString("5f2eac08-bd1f-4635-a6c2-437369f996cf"));
    private static final RuleNodeId RULE_NODE_ID = new RuleNodeId(UUID.fromString("d46bb666-ecab-4d89-a28f-5abdca23ac29"));

    private static final String SERVICE_ID_STR = "test-service-id";
    private static final String TEST_TOPIC = "test-topic";
    private static final String TEST_KEY = "test-key";
    private static final String TEST_TAG = "test-tag";
    private static final String TEST_PRODUCER_GROUP = "test-producer-group";
    private static final String MSG_ID = "test-msg-id";
    private static final String BROKER_NAME = "broker-a";
    private static final int QUEUE_ID = 3;
    private static final long QUEUE_OFFSET = 42L;

    private JnksIotRocketMqNode node;
    private JnksIotRocketMqNodeConfiguration config;
    private SendResult sendResult;

    @Mock(strictness = Mock.Strictness.LENIENT)
    private JnksIotContext ctxMock;
    @Mock
    private DefaultMQProducer producerMock;

    @BeforeEach
    public void setUp() {
        node = spy(new JnksIotRocketMqNode());
        config = new JnksIotRocketMqNodeConfiguration().defaultConfiguration();
        config.setTopicPattern(TEST_TOPIC);
        config.setKeyPattern(TEST_KEY);
        config.setTagPattern(TEST_TAG);
        sendResult = new SendResult(SendStatus.SEND_OK, MSG_ID, "offset-msg-id",
                new MessageQueue(TEST_TOPIC, BROKER_NAME, QUEUE_ID), QUEUE_OFFSET);
    }

    @Test
    public void verifyDefaultConfig() {
        config = new JnksIotRocketMqNodeConfiguration().defaultConfiguration();
        assertThat(config.getTopicPattern()).isEqualTo("my-topic");
        assertThat(config.getKeyPattern()).isNull();
        assertThat(config.getTagPattern()).isNull();
        assertThat(config.getNameServerAddress()).isEqualTo("localhost:9876");
        assertThat(config.getProducerGroup()).isEqualTo("jnks-iot-rule-node-producer");
        assertThat(config.getSendMode()).isEqualTo("ASYNC");
        assertThat(config.getSendMsgTimeout()).isEqualTo(3000);
        assertThat(config.getRetryTimes()).isEqualTo(2);
        assertThat(config.getNamespace()).isNull();
        assertThat(config.getAccessKey()).isNull();
        assertThat(config.getSecretKey()).isNull();
        assertThat(config.isAddMetadataKeyValuesAsProperties()).isFalse();
    }

    @Test
    public void verifyProducerConfig() {
        config.setProducerGroup(TEST_PRODUCER_GROUP);
        config.setSendMsgTimeout(5000);
        config.setRetryTimes(3);
        config.setNamespace("test-namespace");
        given(ctxMock.getSelfId()).willReturn(RULE_NODE_ID);
        given(ctxMock.getServiceId()).willReturn(SERVICE_ID_STR);
        ReflectionTestUtils.setField(node, "config", config);

        DefaultMQProducer producer = node.getProducer(ctxMock);

        assertThat(producer.getNamesrvAddr()).isEqualTo(config.getNameServerAddress());
        assertThat(producer.getProducerGroup()).isEqualTo(TEST_PRODUCER_GROUP);
        assertThat(producer.getNamespace()).isEqualTo("test-namespace");
        assertThat(producer.getInstanceName())
                .isEqualTo("producer-tb-rocketmq-node-" + RULE_NODE_ID.getId() + "-" + SERVICE_ID_STR);
        assertThat(producer.getSendMsgTimeout()).isEqualTo(5000);
        assertThat(producer.getRetryTimesWhenSendFailed()).isEqualTo(3);
    }

    @Test
    public void givenAsyncSend_whenOnMsg_thenTellSuccessWithSendResult() throws Exception {
        mockSuccessfulInit();
        willAnswer(invocation -> {
            SendCallback callback = invocation.getArgument(1);
            callback.onSuccess(sendResult);
            return null;
        }).given(producerMock).send(any(Message.class), any(SendCallback.class));

        JnksIotMsg msg = buildMsg(new JnksIotMsgMetaData());
        node.onMsg(ctxMock, msg);

        Message message = captureMessage();
        assertThat(message.getTopic()).isEqualTo(TEST_TOPIC);
        assertThat(message.getKeys()).isEqualTo(TEST_KEY);
        assertThat(message.getTags()).isEqualTo(TEST_TAG);
        assertThat(message.getBody()).isEqualTo(msg.getData().getBytes(StandardCharsets.UTF_8));
        then(ctxMock).should(never()).ack(msg);

        ArgumentCaptor<JnksIotMsg> actualMsg = ArgumentCaptor.forClass(JnksIotMsg.class);
        then(ctxMock).should().tellSuccess(actualMsg.capture());
        verifyOutgoingSuccessMsg(actualMsg.getValue(), msg);
    }

    @Test
    public void givenSyncSend_whenOnMsg_thenTellSuccessWithSendResult() throws Exception {
        config.setSendMode(JnksIotRocketMqNode.SendMode.SYNC.name());
        mockSuccessfulInit();
        given(producerMock.send(any(Message.class))).willReturn(sendResult);

        JnksIotMsg msg = buildMsg(new JnksIotMsgMetaData());
        node.onMsg(ctxMock, msg);

        then(producerMock).should().send(any(Message.class));

        ArgumentCaptor<JnksIotMsg> actualMsg = ArgumentCaptor.forClass(JnksIotMsg.class);
        then(ctxMock).should().tellSuccess(actualMsg.capture());
        verifyOutgoingSuccessMsg(actualMsg.getValue(), msg);
    }

    @Test
    public void givenOnewaySend_whenOnMsg_thenTellSuccessWithoutSendResult() throws Exception {
        config.setSendMode(JnksIotRocketMqNode.SendMode.ONEWAY.name());
        mockSuccessfulInit();

        JnksIotMsg msg = buildMsg(new JnksIotMsgMetaData());
        node.onMsg(ctxMock, msg);

        then(producerMock).should().sendOneway(any(Message.class));
        then(ctxMock).should().tellSuccess(msg);
    }

    @Test
    public void givenSendFailed_whenOnMsg_thenTellFailure() throws Exception {
        mockSuccessfulInit();
        RuntimeException error = new RuntimeException("Something went wrong!");
        willAnswer(invocation -> {
            SendCallback callback = invocation.getArgument(1);
            callback.onException(error);
            return null;
        }).given(producerMock).send(any(Message.class), any(SendCallback.class));

        JnksIotMsg msg = buildMsg(new JnksIotMsgMetaData());
        node.onMsg(ctxMock, msg);

        ArgumentCaptor<JnksIotMsg> actualMsg = ArgumentCaptor.forClass(JnksIotMsg.class);
        ArgumentCaptor<Throwable> actualError = ArgumentCaptor.forClass(Throwable.class);
        then(ctxMock).should().tellFailure(actualMsg.capture(), actualError.capture());
        assertThat(actualError.getValue()).isSameAs(error);
        assertThat(actualMsg.getValue().getMetaData().getValue("error"))
                .isEqualTo(RuntimeException.class + ": " + error.getMessage());
    }

    @Test
    public void givenSyncSendThrows_whenOnMsg_thenTellFailure() throws Exception {
        config.setSendMode(JnksIotRocketMqNode.SendMode.SYNC.name());
        mockSuccessfulInit();
        RuntimeException error = new RuntimeException("Something went wrong!");
        willThrow(error).given(producerMock).send(any(Message.class));

        JnksIotMsg msg = buildMsg(new JnksIotMsgMetaData());
        node.onMsg(ctxMock, msg);

        ArgumentCaptor<Throwable> actualError = ArgumentCaptor.forClass(Throwable.class);
        then(ctxMock).should().tellFailure(any(JnksIotMsg.class), actualError.capture());
        assertThat(actualError.getValue()).isSameAs(error);
    }

    @Test
    public void givenAddMetadataKeyValuesAsProperties_whenOnMsg_thenMessageContainsSanitizedProperties() throws Exception {
        config.setAddMetadataKeyValuesAsProperties(true);
        mockSuccessfulInit();
        willAnswer(invocation -> {
            SendCallback callback = invocation.getArgument(1);
            callback.onSuccess(sendResult);
            return null;
        }).given(producerMock).send(any(Message.class), any(SendCallback.class));

        JnksIotMsgMetaData metaData = new JnksIotMsgMetaData();
        metaData.putValue("deviceName", "Test device");
        metaData.putValue("device.profile", "default");
        node.onMsg(ctxMock, buildMsg(metaData));

        Message message = captureMessage();
        assertThat(message.getUserProperty("jnks_iot_msg_md_deviceName")).isEqualTo("Test device");
        assertThat(message.getUserProperty("jnks_iot_msg_md_device_profile")).isEqualTo("default");
    }

    @Test
    public void givenProducerStartFailed_whenInit_thenThrowNodeException() throws Exception {
        willReturn(producerMock).given(node).getProducer(any());
        willThrow(new IllegalStateException("Failed to start")).given(producerMock).start();

        assertThatThrownBy(() -> node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config))))
                .isInstanceOf(JnksIotNodeException.class);
    }

    @Test
    public void givenProducerIsNotNull_whenDestroy_thenShutdown() {
        ReflectionTestUtils.setField(node, "producer", producerMock);
        node.destroy();
        then(producerMock).should().shutdown();
    }

    @Test
    public void givenProducerIsNull_whenDestroy_thenDoNothing() {
        node.destroy();
        then(producerMock).shouldHaveNoInteractions();
    }

    private void mockSuccessfulInit() throws JnksIotNodeException {
        willReturn(producerMock).given(node).getProducer(any());
        given(ctxMock.getExternalCallExecutor()).willReturn(new TestDbCallbackExecutor());
        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));
    }

    private Message captureMessage() throws Exception {
        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        then(producerMock).should().send(messageCaptor.capture(), any(SendCallback.class));
        return messageCaptor.getValue();
    }

    private JnksIotMsg buildMsg(JnksIotMsgMetaData metaData) {
        return JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(metaData)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();
    }

    private void verifyOutgoingSuccessMsg(JnksIotMsg actualMsg, JnksIotMsg originalMsg) {
        JnksIotMsgMetaData metaData = originalMsg.getMetaData().copy();
        metaData.putValue("msgId", MSG_ID);
        metaData.putValue("topic", TEST_TOPIC);
        metaData.putValue("queueId", String.valueOf(QUEUE_ID));
        metaData.putValue("queueOffset", String.valueOf(QUEUE_OFFSET));
        metaData.putValue("status", SendStatus.SEND_OK.name());
        JnksIotMsg expectedMsg = originalMsg.transform()
                .metaData(metaData)
                .build();
        assertThat(actualMsg)
                .usingRecursiveComparison()
                .ignoringFields("ctx")
                .isEqualTo(expectedMsg);
    }
}
