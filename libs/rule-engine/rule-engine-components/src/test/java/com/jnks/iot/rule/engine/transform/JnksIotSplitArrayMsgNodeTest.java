package com.jnks.iot.rule.engine.transform;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.EmptyNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;
import com.jnks.iot.server.common.msg.queue.JnksIotMsgCallback;

import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class JnksIotSplitArrayMsgNodeTest {
    DeviceId deviceId;
    JnksIotSplitArrayMsgNode node;
    EmptyNodeConfiguration config;
    JnksIotNodeConfiguration nodeConfiguration;
    JnksIotContext ctx;
    JnksIotMsgCallback callback;

    @BeforeEach
    void setUp() throws JnksIotNodeException {
        deviceId = new DeviceId(UUID.randomUUID());
        callback = mock(JnksIotMsgCallback.class);
        ctx = mock(JnksIotContext.class);
        config = new EmptyNodeConfiguration();
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        node = spy(new JnksIotSplitArrayMsgNode());
        node.init(ctx, nodeConfiguration);
    }

    @AfterEach
    void tearDown() {
        node.destroy();
    }

    @Test
    void givenFewMsg_whenOnMsg_thenVerifyOutput() throws Exception {
        String data = "[{\"Attribute_1\":22.5,\"Attribute_2\":10.3}, {\"Attribute_1\":1,\"Attribute_2\":2}]";
        VerifyOutputMsg(data);
    }

    @Test
    void givenOneMsg_whenOnMsg_thenVerifyOutput() throws Exception {
        String data = "[{\"Attribute_1\":22.5,\"Attribute_2\":10.3}]";
        VerifyOutputMsg(data);
    }

    @Test
    void givenZeroMsg_whenOnMsg_thenVerifyOutput() throws Exception {
        VerifyOutputMsg(JnksIotMsg.EMPTY_JSON_ARRAY);
    }

    @Test
    void givenNoArrayMsg_whenOnMsg_thenFailure() throws Exception {
        String data = "{\"Attribute_1\":22.5,\"Attribute_2\":10.3}";
        JsonNode dataNode = JacksonUtil.toJsonNode(data);
        JnksIotMsg msg = getJnksIotMsg(deviceId, dataNode.toString());
        node.onMsg(ctx, msg);

        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(ctx, never()).tellSuccess(any());
        verify(ctx, never()).enqueueForTellNext(any(), anyString(), any(), any());
        verify(ctx, times(1)).tellFailure(newMsgCaptor.capture(), exceptionCaptor.capture());

        assertThat(exceptionCaptor.getValue()).isInstanceOf(RuntimeException.class);

        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();

        assertThat(newMsg).isSameAs(msg);
    }

    private void VerifyOutputMsg(String data) throws Exception {
        JsonNode dataNode = JacksonUtil.toJsonNode(data);
        JnksIotMsg jnksIotMsg = getJnksIotMsg(deviceId, dataNode.toString());
        node.onMsg(ctx, jnksIotMsg);

        if (dataNode.size() > 1) {
            ArgumentCaptor<Runnable> successCaptor = ArgumentCaptor.forClass(Runnable.class);
            ArgumentCaptor<Consumer<Throwable>> failureCaptor = ArgumentCaptor.forClass(Consumer.class);
            verify(ctx, times(dataNode.size())).enqueueForTellNext(any(), anyString(), successCaptor.capture(), failureCaptor.capture());
            for (Runnable valueCaptor : successCaptor.getAllValues()) {
                valueCaptor.run();
            }
            verify(ctx, times(1)).ack(jnksIotMsg);
        } else {
            ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
            verify(ctx, times(dataNode.size())).tellSuccess(newMsgCaptor.capture());
        }
        verify(ctx, never()).tellFailure(any(), any());
    }

    private JnksIotMsg getJnksIotMsg(EntityId entityId, String data) {
        Map<String, String> mdMap = Map.of(
                "country", "US",
                "city", "NY"
        );
        return JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_ATTRIBUTES_REQUEST)
                .originator(entityId)
                .copyMetaData(new JnksIotMsgMetaData(mdMap))
                .data(data)
                .callback(callback)
                .build();
    }
}
