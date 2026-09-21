package com.jnks.iot.rule.engine.flow;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.AbstractRuleNodeUpgradeTest;
import com.jnks.iot.rule.engine.api.EmptyNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.DataConstants;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.spy;
import static org.mockito.BDDMockito.then;

@Slf4j
@ExtendWith(MockitoExtension.class)
public class JnksIotCheckpointNodeTest extends AbstractRuleNodeUpgradeTest {

    private final DeviceId DEVICE_ID = new DeviceId(UUID.fromString("37840655-b7dc-4f49-8da3-9429159e0970"));

    private JnksIotCheckpointNode node;
    private EmptyNodeConfiguration config;
    private JnksIotNodeConfiguration nodeConfiguration;

    @Mock
    private JnksIotContext ctxMock;

    @BeforeEach
    public void setUp() {
        node = spy(new JnksIotCheckpointNode());
        config = new EmptyNodeConfiguration().defaultConfiguration();
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
    }

    @Test
    public void verifyDefaultConfig() {
        assertThat(config.getVersion()).isEqualTo(0);
    }

    @Test
    public void givenDefaultConfig_whenInit_thenOk() {
        assertThatNoException().isThrownBy(() -> node.init(ctxMock, nodeConfiguration));
    }

    @ParameterizedTest
    @ValueSource(strings = {DataConstants.MAIN_QUEUE_NAME, DataConstants.HP_QUEUE_NAME, DataConstants.SQ_QUEUE_NAME, "Custom queue"})
    public void givenQueueName_whenOnMsg_thenTransfersMsgToDefinedQueue(String queueName) throws JnksIotNodeException {
        given(ctxMock.getQueueName()).willReturn(queueName);

        node.init(ctxMock, nodeConfiguration);
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();
        node.onMsg(ctxMock, msg);

        ArgumentCaptor<Runnable> onSuccess = ArgumentCaptor.forClass(Runnable.class);
        then(ctxMock).should().enqueueForTellNext(eq(msg), eq(queueName), eq(JnksIotNodeConnectionType.SUCCESS), onSuccess.capture(), any());
        onSuccess.getValue().run();
        then(ctxMock).should().ack(msg);
    }

    @Test
    public void givenErrorDuringTransfer_whenOnMsg_thenTellFailure() throws JnksIotNodeException {
        given(ctxMock.getQueueName()).willReturn(DataConstants.HP_QUEUE_NAME);

        node.init(ctxMock, nodeConfiguration);
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();
        node.onMsg(ctxMock, msg);

        ArgumentCaptor<Consumer<Throwable>> onFailure = ArgumentCaptor.forClass(Consumer.class);
        then(ctxMock).should().enqueueForTellNext(eq(msg), eq(DataConstants.HP_QUEUE_NAME), eq(JnksIotNodeConnectionType.SUCCESS), any(), onFailure.capture());
        String errorMsg = "Something went wrong.";
        onFailure.getValue().accept(new RuntimeException(errorMsg));
        ArgumentCaptor<Throwable> throwable = ArgumentCaptor.forClass(Throwable.class);
        then(ctxMock).should().tellFailure(eq(msg), throwable.capture());
        assertThat(throwable.getValue()).isInstanceOf(RuntimeException.class).hasMessage(errorMsg);
    }

    // Rule nodes upgrade
    private static Stream<Arguments> givenFromVersionAndConfig_whenUpgrade_thenVerifyHasChangesAndConfig() {
        return Stream.of(
                // default config for version 0
                Arguments.of(0,
                        "{\"queueName\":null}",
                        true,
                        "{}"),
                // default config for version 0 with queueName
                Arguments.of(0,
                        "{\"queueName\":\"Main\"}",
                        true,
                        "{}"),
                // default config for version 1 with upgrade from version 0
                Arguments.of(0,
                        "{}",
                        false,
                        "{}")
        );
    }

    @Override
    protected JnksIotNode getTestNode() {
        return node;
    }
}
