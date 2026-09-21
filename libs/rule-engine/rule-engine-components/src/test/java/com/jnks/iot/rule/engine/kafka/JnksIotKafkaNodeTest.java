package com.jnks.iot.rule.engine.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.KafkaThread;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.common.util.ListeningExecutor;
import com.jnks.iot.rule.engine.AbstractRuleNodeUpgradeTest;
import com.jnks.iot.rule.engine.TestDbCallbackExecutor;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.exception.JnksIotKafkaClientError;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.RuleNodeId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.mock;
import static org.mockito.BDDMockito.never;
import static org.mockito.BDDMockito.spy;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.times;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.BDDMockito.willThrow;

@ExtendWith(MockitoExtension.class)
public class JnksIotKafkaNodeTest extends AbstractRuleNodeUpgradeTest {

    private final DeviceId DEVICE_ID = new DeviceId(UUID.fromString("5f2eac08-bd1f-4635-a6c2-437369f996cf"));
    private final RuleNodeId RULE_NODE_ID = new RuleNodeId(UUID.fromString("d46bb666-ecab-4d89-a28f-5abdca23ac29"));
    private final ListeningExecutor executor = new TestDbCallbackExecutor();

    private final long OFFSET = 1;
    private final int PARTITION = 0;

    private final String SERVICE_ID_STR = "test-service-id";
    private final String TEST_TOPIC = "test-topic";
    private final String TEST_KEY = "test-key";

    private JnksIotKafkaNode node;
    private JnksIotKafkaNodeConfiguration config;

    @Mock
    private JnksIotContext ctxMock;
    @Mock
    private KafkaProducer<String, String> producerMock;
    @Mock
    private KafkaThread ioThreadMock;
    @Mock
    private RecordMetadata recordMetadataMock;

    @BeforeEach
    public void setUp() {
        node = spy(new JnksIotKafkaNode());
        config = new JnksIotKafkaNodeConfiguration().defaultConfiguration();
        config.setTopicPattern(TEST_TOPIC);
        config.setKeyPattern(TEST_KEY);
    }

    @Test
    public void verifyDefaultConfig() {
        config = new JnksIotKafkaNodeConfiguration().defaultConfiguration();
        assertThat(config.getTopicPattern()).isEqualTo("my-topic");
        assertThat(config.getKeyPattern()).isNull();
        assertThat(config.getBootstrapServers()).isEqualTo("localhost:9092");
        assertThat(config.getRetries()).isEqualTo(0);
        assertThat(config.getBatchSize()).isEqualTo(16384);
        assertThat(config.getLinger()).isEqualTo(0);
        assertThat(config.getBufferMemory()).isEqualTo(33554432);
        assertThat(config.getAcks()).isEqualTo("-1");
        assertThat(config.getOtherProperties()).isEmpty();
        assertThat(config.isAddMetadataKeyValuesAsKafkaHeaders()).isFalse();
        assertThat(config.getKafkaHeadersCharset()).isEqualTo("UTF-8");
    }

    @Test
    public void givenExceptionDuringKafkaInitialization_whenInit_thenDestroy() throws JnksIotNodeException {
        // GIVEN
        given(ctxMock.getSelfId()).willReturn(RULE_NODE_ID);
        ReflectionTestUtils.setField(producerMock, "ioThread", ioThreadMock);
        willAnswer(invocationOnMock -> {
            Thread.UncaughtExceptionHandler exceptionHandler = invocationOnMock.getArgument(0);
            exceptionHandler.uncaughtException(ioThreadMock, new JnksIotKafkaClientError("Error during init"));
            return null;
        }).given(ioThreadMock).setUncaughtExceptionHandler(any());
        willReturn(producerMock).given(node).getKafkaProducer(any());

        // WHEN
        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        // THEN
        then(producerMock).should().close();
        then(producerMock).shouldHaveNoMoreInteractions();
    }

    @Test
    public void verifyKafkaProperties() throws JnksIotNodeException {
        String sslKeyStoreCertificateChain = "cbdvch\\nfwrg\nvgwg\\n";
        String sslKeyStoreKey = "nghmh\\nhmmnh\\\\ngreg\nvgwg\\n";
        String sslTruststoreCertificates = "grthrt\fd\\nfwrg\nvgwg\\n";
        config.setOtherProperties(Map.of(
                "ssl.keystore.certificate.chain", sslKeyStoreCertificateChain,
                "ssl.keystore.key", sslKeyStoreKey,
                "ssl.truststore.certificates", sslTruststoreCertificates,
                "ssl.protocol", "TLSv1.2"
        ));

        mockSuccessfulInit();

        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        Properties expectedProperties = new Properties();
        expectedProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-tb-kafka-node-" + RULE_NODE_ID.getId() + "-" + SERVICE_ID_STR);
        expectedProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        expectedProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        expectedProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        expectedProperties.put(ProducerConfig.ACKS_CONFIG, config.getAcks());
        expectedProperties.put(ProducerConfig.RETRIES_CONFIG, config.getRetries());
        expectedProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, config.getBatchSize());
        expectedProperties.put(ProducerConfig.LINGER_MS_CONFIG, config.getLinger());
        expectedProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.getBufferMemory());
        expectedProperties.put("ssl.keystore.certificate.chain", sslKeyStoreCertificateChain.replace("\\n", "\n"));
        expectedProperties.put("ssl.keystore.key", sslKeyStoreKey.replace("\\n", "\n"));
        expectedProperties.put("ssl.truststore.certificates", sslTruststoreCertificates.replace("\\n", "\n"));
        expectedProperties.put("ssl.protocol", "TLSv1.2");

        ArgumentCaptor<Properties> properties = ArgumentCaptor.forClass(Properties.class);
        then(node).should().getKafkaProducer(properties.capture());
        assertThat(properties.getValue()).isEqualTo(expectedProperties);
    }

    @Test
    public void givenInitErrorIsNotNull_whenOnMsg_thenTellFailure() {
        // GIVEN
        String errorMsg = "Error during kafka initialization!";
        ReflectionTestUtils.setField(node, "config", config);
        ReflectionTestUtils.setField(node, "initError", new JnksIotKafkaClientError(errorMsg));

        // WHEN
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();
        node.onMsg(ctxMock, msg);

        // THEN
        ArgumentCaptor<Throwable> actualError = ArgumentCaptor.forClass(Throwable.class);
        then(ctxMock).should().tellFailure(eq(msg), actualError.capture());
        assertThat(actualError.getValue())
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Failed to initialize Kafka rule node producer: " + errorMsg);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void givenForceAckAndExceptionWasThrown_whenOnMsg_thenTellFailure(boolean forceAck) throws JnksIotNodeException {
        // GIVEN
        given(ctxMock.isExternalNodeForceAck()).willReturn(forceAck);
        mockSuccessfulInit();
        ListeningExecutor executorMock = mock(ListeningExecutor.class);
        given(ctxMock.getExternalCallExecutor()).willReturn(executorMock);
        String errorMsg = "Something went wrong!";
        willThrow(new RuntimeException(errorMsg)).given(executorMock).executeAsync(any(Callable.class));

        // WHEN
        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();
        node.onMsg(ctxMock, msg);

        // THEN
        then(ctxMock).should(forceAck ? times(1) : never()).ack(msg);
        ArgumentCaptor<JnksIotMsg> actualMsg = ArgumentCaptor.forClass(JnksIotMsg.class);
        ArgumentCaptor<Throwable> actualError = ArgumentCaptor.forClass(Throwable.class);
        then(ctxMock).should().tellFailure(actualMsg.capture(), actualError.capture());
        assertThat(actualMsg.getValue()).usingRecursiveComparison().ignoringFields("ctx").isEqualTo(msg);
        assertThat(actualError.getValue()).isInstanceOf(RuntimeException.class).hasMessage(errorMsg);
    }

    @ParameterizedTest
    @MethodSource
    public void givenForceAckIsTrueTopicAndKeyPatternsAndAddMetadataKeyValuesAsKafkaHeadersIsFalse_whenOnMsg_thenEnqueueForTellNext(
            String topicPattern, String keyPattern, JnksIotMsgMetaData metaData, String data
    ) throws JnksIotNodeException {
        // GIVEN
        config.setTopicPattern(topicPattern);
        config.setKeyPattern(keyPattern);
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(metaData)
                .data(data)
                .build();
        String topic = JnksIotNodeUtils.processPattern(topicPattern, msg);
        String key = JnksIotNodeUtils.processPattern(keyPattern, msg);

        given(ctxMock.isExternalNodeForceAck()).willReturn(true);
        mockSuccessfulInit();
        mockSuccessfulPublishingRequest(topic);

        // WHEN
        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));
        node.onMsg(ctxMock, msg);

        // THEN
        then(ctxMock).should().ack(msg);
        verifyProducerRecord(topic, key, msg.getData());
        ArgumentCaptor<JnksIotMsg> actualMsg = ArgumentCaptor.forClass(JnksIotMsg.class);
        then(ctxMock).should().enqueueForTellNext(actualMsg.capture(), eq(JnksIotNodeConnectionType.SUCCESS));
        verifyOutgoingSuccessMsg(topic, actualMsg.getValue(), msg);
    }

    private static Stream<Arguments> givenForceAckIsTrueTopicAndKeyPatternsAndAddMetadataKeyValuesAsKafkaHeadersIsFalse_whenOnMsg_thenEnqueueForTellNext() {
        return Stream.of(
                Arguments.of("test-topic", "test-key", new JnksIotMsgMetaData(), JnksIotMsg.EMPTY_JSON_OBJECT),
                Arguments.of("${mdTopicPattern}", "${mdKeyPattern}", new JnksIotMsgMetaData(
                        Map.of(
                                "mdTopicPattern", "md-test-topic",
                                "mdKeyPattern", "md-test-key"
                        )), JnksIotMsg.EMPTY_JSON_OBJECT),
                Arguments.of("$[msgTopicPattern]", "$[msgKeyPattern]", new JnksIotMsgMetaData(),
                        "{\"msgTopicPattern\":\"msg-test-topic\",\"msgKeyPattern\":\"msg-test-key\"}")
        );
    }

    @ParameterizedTest
    @NullAndEmptySource
    public void givenForceAckIsFalseAndKeyIsNullOrEmptyAndErrorOccursDuringPublishing_whenOnMsg_thenTellFailure(String key) throws JnksIotNodeException {
        // GIVEN
        config.setKeyPattern(key);

        given(ctxMock.isExternalNodeForceAck()).willReturn(false);
        mockSuccessfulInit();
        String errorMsg = "Something went wrong!";
        mockFailedPublishingRequest(new RuntimeException(errorMsg));

        // WHEN
        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();
        node.onMsg(ctxMock, msg);

        // THEN
        verifyProducerRecord(TEST_TOPIC, null, msg.getData());
        then(ctxMock).should(never()).ack(msg);
        ArgumentCaptor<JnksIotMsg> actualMsg = ArgumentCaptor.forClass(JnksIotMsg.class);
        ArgumentCaptor<Throwable> actualError = ArgumentCaptor.forClass(Throwable.class);
        then(ctxMock).should().tellFailure(actualMsg.capture(), actualError.capture());
        verifyOutgoingFailureMsg(errorMsg, actualMsg.getValue(), msg);
    }

    @Test
    public void givenForceAckIsTrueAndAddKafkaHeadersIsTrueAndToBytesCharsetIsNullAndErrorOccursDuringPublishing_whenOnMsg_thenEnqueueForTellFailure() throws JnksIotNodeException {
        // GIVEN
        config.setAddMetadataKeyValuesAsKafkaHeaders(true);
        config.setKafkaHeadersCharset(null);

        given(ctxMock.isExternalNodeForceAck()).willReturn(true);
        mockSuccessfulInit();
        String errorMsg = "Something went wrong!";
        mockFailedPublishingRequest(new RuntimeException(errorMsg));

        // WHEN
        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();
        node.onMsg(ctxMock, msg);

        // THEN
        then(ctxMock).should().ack(msg);
        Headers expectedHeaders = new RecordHeaders();
        msg.getMetaData().values().forEach((k, v) -> expectedHeaders.add(new RecordHeader("jnks_iot_msg_md_" + k, v.getBytes(StandardCharsets.UTF_8))));
        verifyProducerRecord(TEST_TOPIC, TEST_KEY, msg.getData(), expectedHeaders);
        ArgumentCaptor<JnksIotMsg> actualMsg = ArgumentCaptor.forClass(JnksIotMsg.class);
        ArgumentCaptor<Throwable> actualError = ArgumentCaptor.forClass(Throwable.class);
        then(ctxMock).should().enqueueForTellFailure(actualMsg.capture(), actualError.capture());
        verifyOutgoingFailureMsg(errorMsg, actualMsg.getValue(), msg);
    }

    @Test
    public void givenForceAckIsFalseAndAddMetadataKeyValuesAsKafkaHeadersIsTrueAndToBytesCharsetIsSet_whenOnMsg_thenTellSuccess() throws JnksIotNodeException {
        // GIVEN
        config.setAddMetadataKeyValuesAsKafkaHeaders(true);
        config.setKafkaHeadersCharset("UTF-16");

        given(ctxMock.isExternalNodeForceAck()).willReturn(false);
        mockSuccessfulInit();
        mockSuccessfulPublishingRequest(TEST_TOPIC);

        // WHEN
        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));
        JnksIotMsgMetaData metaData = new JnksIotMsgMetaData();
        metaData.putValue("key", "value");
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(metaData)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();
        node.onMsg(ctxMock, msg);

        // THEN
        then(ctxMock).should(never()).ack(msg);
        Headers expectedHeaders = new RecordHeaders();
        msg.getMetaData().values().forEach((k, v) -> expectedHeaders.add(new RecordHeader("jnks_iot_msg_md_" + k, v.getBytes(StandardCharsets.UTF_16))));
        verifyProducerRecord(TEST_TOPIC, TEST_KEY, msg.getData(), expectedHeaders);
        ArgumentCaptor<JnksIotMsg> actualMsg = ArgumentCaptor.forClass(JnksIotMsg.class);
        then(ctxMock).should().tellSuccess(actualMsg.capture());
        verifyOutgoingSuccessMsg(TEST_TOPIC, actualMsg.getValue(), msg);
    }

    @Test
    public void givenProducerIsNotNull_whenDestroy_thenShouldClose() {
        ReflectionTestUtils.setField(node, "producer", producerMock);
        node.destroy();
        then(producerMock).should().close();
    }

    @Test
    public void givenProducerIsNull_whenDestroy_thenDoNothing() {
        node.destroy();
        then(producerMock).shouldHaveNoInteractions();
    }

    private void mockSuccessfulInit() {
        given(ctxMock.getSelfId()).willReturn(RULE_NODE_ID);
        given(ctxMock.getServiceId()).willReturn(SERVICE_ID_STR);
        ReflectionTestUtils.setField(producerMock, "ioThread", ioThreadMock);
        willReturn(producerMock).given(node).getKafkaProducer(any());
    }

    private void mockSuccessfulPublishingRequest(String topic) {
        given(ctxMock.getExternalCallExecutor()).willReturn(executor);
        willAnswer(invocation -> {
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(recordMetadataMock, null);
            return null;
        }).given(producerMock).send(any(), any(Callback.class));
        given(recordMetadataMock.offset()).willReturn(OFFSET);
        given(recordMetadataMock.partition()).willReturn(PARTITION);
        given(recordMetadataMock.topic()).willReturn(topic);
    }

    private void mockFailedPublishingRequest(Exception exception) {
        given(ctxMock.getExternalCallExecutor()).willReturn(executor);
        willAnswer(invocation -> {
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(recordMetadataMock, exception);
            return null;
        }).given(producerMock).send(any(), any(Callback.class));
    }

    private void verifyProducerRecord(String expectedTopic, String expectedKey, String expectedValue) {
        verifyProducerRecord(expectedTopic, expectedKey, expectedValue, null);
    }

    private void verifyProducerRecord(String expectedTopic, String expectedKey, String expectedValue, Headers expectedHeaders) {
        ArgumentCaptor<ProducerRecord<String, String>> actualRecordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        then(producerMock).should().send(actualRecordCaptor.capture(), any());
        ProducerRecord<String, String> actualRecord = actualRecordCaptor.getValue();
        assertThat(actualRecord.topic()).isEqualTo(expectedTopic);
        assertThat(actualRecord.key()).isEqualTo(expectedKey);
        assertThat(actualRecord.value()).isEqualTo(expectedValue);
        if (expectedHeaders != null) {
            assertThat(actualRecord.headers()).isEqualTo(expectedHeaders);
        }
    }

    private void verifyOutgoingSuccessMsg(String expectedTopic, JnksIotMsg actualMsg, JnksIotMsg originalMsg) {
        JnksIotMsgMetaData metaData = originalMsg.getMetaData().copy();
        metaData.putValue("offset", String.valueOf(OFFSET));
        metaData.putValue("partition", String.valueOf(PARTITION));
        metaData.putValue("topic", expectedTopic);
        JnksIotMsg expectedMsg = originalMsg.transform()
                .metaData(metaData)
                .build();
        assertThat(actualMsg)
                .usingRecursiveComparison()
                .ignoringFields("ctx")
                .isEqualTo(expectedMsg);
    }

    private void verifyOutgoingFailureMsg(String errorMsg, JnksIotMsg actualMsg, JnksIotMsg originalMsg) {
        JnksIotMsgMetaData metaData = originalMsg.getMetaData();
        metaData.putValue("error", RuntimeException.class + ": " + errorMsg);
        JnksIotMsg expectedMsg = originalMsg.transform()
                .metaData(metaData)
                .build();
        assertThat(actualMsg).usingRecursiveComparison().ignoringFields("ctx").isEqualTo(expectedMsg);
    }

    private static Stream<Arguments> givenFromVersionAndConfig_whenUpgrade_thenVerifyHasChangesAndConfig() {
        return Stream.of(
                //config for version 0
                Arguments.of(0,
                        "{\n" +
                                "  \"topicPattern\": \"test-topic\",\n" +
                                "  \"keyPattern\": \"test-key\",\n" +
                                "  \"bootstrapServers\": \"localhost:9092\",\n" +
                                "  \"retries\": 0,\n" +
                                "  \"batchSize\": 16384,\n" +
                                "  \"linger\": 0,\n" +
                                "  \"bufferMemory\": 33554432,\n" +
                                "  \"acks\": \"-1\",\n" +
                                "  \"otherProperties\": {},\n" +
                                "  \"addMetadataKeyValuesAsKafkaHeaders\": false,\n" +
                                "  \"kafkaHeadersCharset\": \"UTF-8\",\n" +
                                "  \"keySerializer\": \"org.apache.kafka.common.serialization.StringSerializer\",\n" +
                                "  \"valueSerializer\": \"org.apache.kafka.common.serialization.StringSerializer\"\n" +
                                "}",
                        true,
                        "{\n" +
                                "  \"topicPattern\": \"test-topic\",\n" +
                                "  \"keyPattern\": \"test-key\",\n" +
                                "  \"bootstrapServers\": \"localhost:9092\",\n" +
                                "  \"retries\": 0,\n" +
                                "  \"batchSize\": 16384,\n" +
                                "  \"linger\": 0,\n" +
                                "  \"bufferMemory\": 33554432,\n" +
                                "  \"acks\": \"-1\",\n" +
                                "  \"otherProperties\": {},\n" +
                                "  \"addMetadataKeyValuesAsKafkaHeaders\": false,\n" +
                                "  \"kafkaHeadersCharset\": \"UTF-8\"\n" +
                                "}"
                ),
                //config for version 1 with upgrade from version 0
                Arguments.of(1,
                        "{\n" +
                                "  \"topicPattern\": \"test-topic\",\n" +
                                "  \"keyPattern\": \"test-key\",\n" +
                                "  \"bootstrapServers\": \"localhost:9092\",\n" +
                                "  \"retries\": 0,\n" +
                                "  \"batchSize\": 16384,\n" +
                                "  \"linger\": 0,\n" +
                                "  \"bufferMemory\": 33554432,\n" +
                                "  \"acks\": \"-1\",\n" +
                                "  \"otherProperties\": {},\n" +
                                "  \"addMetadataKeyValuesAsKafkaHeaders\": false,\n" +
                                "  \"kafkaHeadersCharset\": \"UTF-8\"\n" +
                                "}",
                        false,
                        "{\n" +
                                "  \"topicPattern\": \"test-topic\",\n" +
                                "  \"keyPattern\": \"test-key\",\n" +
                                "  \"bootstrapServers\": \"localhost:9092\",\n" +
                                "  \"retries\": 0,\n" +
                                "  \"batchSize\": 16384,\n" +
                                "  \"linger\": 0,\n" +
                                "  \"bufferMemory\": 33554432,\n" +
                                "  \"acks\": \"-1\",\n" +
                                "  \"otherProperties\": {},\n" +
                                "  \"addMetadataKeyValuesAsKafkaHeaders\": false,\n" +
                                "  \"kafkaHeadersCharset\": \"UTF-8\"\n" +
                                "}"
                )
        );
    }

    @Override
    protected JnksIotNode getTestNode() {
        return node;
    }
}
