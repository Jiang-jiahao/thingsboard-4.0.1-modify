package com.jnks.iot.rule.engine.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.TestDbCallbackExecutor;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.RuleNodeId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

/**
 * 需要真实 RocketMQ：设置 ROCKETMQ_NAMESRV_ADDR（如 localhost:9876）后才会执行。
 */
@ExtendWith(MockitoExtension.class)
@EnabledIfEnvironmentVariable(named = "ROCKETMQ_NAMESRV_ADDR", matches = ".+")
public class JnksIotRocketMqNodeIntegrationTest {

    private static final DeviceId DEVICE_ID = new DeviceId(UUID.fromString("5f2eac08-bd1f-4635-a6c2-437369f996cf"));
    private static final RuleNodeId RULE_NODE_ID = new RuleNodeId(UUID.fromString("d46bb666-ecab-4d89-a28f-5abdca23ac29"));

    private final String namesrvAddr = System.getenv("ROCKETMQ_NAMESRV_ADDR");
    private final String suffix = UUID.randomUUID().toString().substring(0, 8);
    private final String topic = "jnks-iot-it-" + suffix;
    private final String producerGroup = "jnks-iot-it-producer-" + suffix;
    private final String consumerGroup = "jnks-iot-it-consumer-" + suffix;

    private JnksIotRocketMqNode node;
    private DefaultMQPushConsumer consumer;

    @Mock(strictness = Mock.Strictness.LENIENT)
    private JnksIotContext ctxMock;

    @BeforeEach
    public void setUp() {
        node = new JnksIotRocketMqNode();
        given(ctxMock.getSelfId()).willReturn(RULE_NODE_ID);
        given(ctxMock.getServiceId()).willReturn("it-service");
        given(ctxMock.getExternalCallExecutor()).willReturn(new TestDbCallbackExecutor());
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (node != null) {
            node.destroy();
        }
        if (consumer != null) {
            consumer.shutdown();
        }
    }

    @Test
    public void givenRealBroker_whenOnMsg_thenMessageIsConsumed() throws Exception {
        JnksIotRocketMqNodeConfiguration config = new JnksIotRocketMqNodeConfiguration().defaultConfiguration();
        config.setTopicPattern(topic);
        config.setKeyPattern("${deviceKey}");
        config.setTagPattern("it-tag");
        config.setNameServerAddress(namesrvAddr);
        config.setProducerGroup(producerGroup);
        config.setAddMetadataKeyValuesAsProperties(true);

        JnksIotMsgMetaData metaData = new JnksIotMsgMetaData();
        metaData.putValue("deviceKey", "device-key-1");
        metaData.putValue("device.name", "Test device");
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(metaData)
                .data("{\"temperature\":42}")
                .build();

        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<MessageExt> received = new AtomicReference<>();
        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            received.set(msgs.get(0));
            latch.countDown();
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();

        node.onMsg(ctxMock, msg);

        assertThat(latch.await(60, TimeUnit.SECONDS)).as("message was not received from RocketMQ").isTrue();
        MessageExt messageExt = received.get();
        assertThat(new String(messageExt.getBody(), StandardCharsets.UTF_8)).isEqualTo("{\"temperature\":42}");
        assertThat(messageExt.getKeys()).isEqualTo("device-key-1");
        assertThat(messageExt.getTags()).isEqualTo("it-tag");
        assertThat(messageExt.getUserProperty("jnks_iot_msg_md_deviceKey")).isEqualTo("device-key-1");
        assertThat(messageExt.getUserProperty("jnks_iot_msg_md_device_name")).isEqualTo("Test device");

        ArgumentCaptor<JnksIotMsg> actualMsg = ArgumentCaptor.forClass(JnksIotMsg.class);
        then(ctxMock).should().tellSuccess(actualMsg.capture());
        assertThat(actualMsg.getValue().getMetaData().getValue("msgId")).isNotBlank();
        assertThat(actualMsg.getValue().getMetaData().getValue("topic")).isEqualTo(topic);
        assertThat(actualMsg.getValue().getMetaData().getValue("status")).isEqualTo("SEND_OK");
    }
}
