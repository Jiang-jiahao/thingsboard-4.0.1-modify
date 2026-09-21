package com.jnks.iot.rule.engine.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.PathNotFoundException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import com.jnks.iot.common.util.JacksonUtil;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class JnksIotJsonPathNodeTest {
    DeviceId deviceId;
    JnksIotJsonPathNode node;
    JnksIotJsonPathNodeConfiguration config;
    JnksIotNodeConfiguration nodeConfiguration;
    JnksIotContext ctx;
    JnksIotMsgCallback callback;

    @BeforeEach
    void setUp() throws JnksIotNodeException {
        deviceId = new DeviceId(UUID.randomUUID());
        callback = mock(JnksIotMsgCallback.class);
        ctx = mock(JnksIotContext.class);
        config = new JnksIotJsonPathNodeConfiguration();
        config.setJsonPath("$.Attribute_2");
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        node = spy(new JnksIotJsonPathNode());
        node.init(ctx, nodeConfiguration);
    }

    @AfterEach
    void tearDown() {
        node.destroy();
    }

    @Test
    void givenDefaultConfig_whenInit_thenFail() {
        config.setJsonPath("");
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        assertThatThrownBy(() -> node.init(ctx, nodeConfiguration)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void givenDefaultConfig_whenVerify_thenOK() {
        JnksIotJsonPathNodeConfiguration defaultConfig = new JnksIotJsonPathNodeConfiguration().defaultConfiguration();
        assertThat(defaultConfig.getJsonPath()).isEqualTo(JnksIotJsonPathNodeConfiguration.DEFAULT_JSON_PATH);
    }

    @Test
    void givenJsonMsg_whenOnMsg_thenVerifyOutputJsonPrimitiveNode() throws Exception {
        String data = "{\"Attribute_1\":22.5,\"Attribute_2\":100}";
        VerifyOutputMsg(data, 1, 100);

        data = "{\"Attribute_1\":22.5,\"Attribute_2\":\"StringValue\"}";
        VerifyOutputMsg(data, 2, "StringValue");
    }

    @Test
    void givenJsonMsg_whenOnMsg_thenVerifyJavaPrimitiveOutput() throws Exception {
        config.setJsonPath("$.attributes.length()");
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        node.init(ctx, nodeConfiguration);

        String data = "{\"attributes\":[{\"attribute_1\":10},{\"attribute_2\":20},{\"attribute_3\":30},{\"attribute_4\":40}]}";
        VerifyOutputMsg(data, 1, 4);

    }

    @Test
    void givenJsonArray_whenOnMsg_thenVerifyOutput() throws Exception {
        String data = "{\"Attribute_1\":22.5,\"Attribute_2\":[{\"Attribute_3\":22.5,\"Attribute_4\":10.3}, {\"Attribute_5\":22.5,\"Attribute_6\":10.3}]}";
        VerifyOutputMsg(data, 1, JacksonUtil.toJsonNode(data).get("Attribute_2"));
    }

    @Test
    void givenJsonNode_whenOnMsg_thenVerifyOutput() throws Exception {
        String data = "{\"Attribute_1\":22.5,\"Attribute_2\":{\"Attribute_3\":22.5,\"Attribute_4\":10.3}}";
        VerifyOutputMsg(data, 1, JacksonUtil.toJsonNode(data).get("Attribute_2"));
    }

    @Test
    void givenJsonArrayWithFilter_whenOnMsg_thenVerifyOutput() throws Exception {
        config.setJsonPath("$.Attribute_2[?(@.voltage > 200)]");
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        node.init(ctx, nodeConfiguration);

        String data = "{\"Attribute_1\":22.5,\"Attribute_2\":[{\"voltage\":220}, {\"voltage\":250}, {\"voltage\":110}]}";
        VerifyOutputMsg(data, 1, JacksonUtil.toJsonNode("[{\"voltage\":220}, {\"voltage\":250}]"));
    }

    @Test
    void givenNoArrayMsg_whenOnMsg_thenTellFailure() throws Exception {
        String data = "{\"Attribute_1\":22.5,\"Attribute_5\":10.3}";
        JsonNode dataNode = JacksonUtil.toJsonNode(data);
        JnksIotMsg msg = getJnksIotMsg(deviceId, dataNode.toString());
        node.onMsg(ctx, msg);

        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(ctx, never()).tellSuccess(any());
        verify(ctx, times(1)).tellFailure(newMsgCaptor.capture(), exceptionCaptor.capture());

        assertThat(newMsgCaptor.getValue()).isSameAs(msg);
        assertThat(exceptionCaptor.getValue()).isInstanceOf(RuntimeException.class);
    }

    @Test
    void givenNoResultsForPath_whenOnMsg_thenTellFailure() throws Exception {
        String data = "{\"Attribute_1\":22.5,\"Attribute_5\":10.3}";
        JsonNode dataNode = JacksonUtil.toJsonNode(data);
        JnksIotMsg msg = getJnksIotMsg(deviceId, dataNode.toString());
        node.onMsg(ctx, msg);

        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(ctx, never()).tellSuccess(any());
        verify(ctx, times(1)).tellFailure(newMsgCaptor.capture(), exceptionCaptor.capture());

        assertThat(newMsgCaptor.getValue()).isSameAs(msg);
        assertThat(exceptionCaptor.getValue()).isInstanceOf(PathNotFoundException.class);
    }

    private void VerifyOutputMsg(String data, int countTellSuccess, Object value) throws Exception {
        JsonNode dataNode = JacksonUtil.toJsonNode(data);
        node.onMsg(ctx, getJnksIotMsg(deviceId, dataNode.toString()));

        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(countTellSuccess)).tellSuccess(newMsgCaptor.capture());
        verify(ctx, never()).tellFailure(any(), any());

        assertThat(newMsgCaptor.getValue().getData()).isEqualTo(JacksonUtil.toString(value));
    }

    private JnksIotMsg getJnksIotMsg(EntityId entityId, String data) {
        Map<String, String> mdMap = Map.of("country", "US",
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
