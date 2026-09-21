package com.jnks.iot.rule.engine.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;
import com.jnks.iot.server.common.msg.queue.JnksIotMsgCallback;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class JnksIotCopyKeysNodeTest {
    DeviceId deviceId;
    JnksIotCopyKeysNode node;
    JnksIotCopyKeysNodeConfiguration config;
    JnksIotNodeConfiguration nodeConfiguration;
    JnksIotContext ctx;
    JnksIotMsgCallback callback;

    @BeforeEach
    void setUp() throws JnksIotNodeException {
        deviceId = new DeviceId(UUID.randomUUID());
        callback = mock(JnksIotMsgCallback.class);
        ctx = mock(JnksIotContext.class);
        config = new JnksIotCopyKeysNodeConfiguration().defaultConfiguration();
        config.setKeys(Set.of("TestKey_1", "TestKey_2", "TestKey_3", "(\\w*)Data(\\w*)"));
        config.setCopyFrom(JnksIotMsgSource.METADATA);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        node = spy(new JnksIotCopyKeysNode());
        node.init(ctx, nodeConfiguration);
    }

    @AfterEach
    void tearDown() {
        node.destroy();
    }

    @Test
    void givenDefaultConfig_whenVerify_thenOK() {
        JnksIotCopyKeysNodeConfiguration defaultConfig = new JnksIotCopyKeysNodeConfiguration().defaultConfiguration();
        assertThat(defaultConfig.getKeys()).isEqualTo(Collections.emptySet());
        assertThat(defaultConfig.getCopyFrom()).isEqualTo(JnksIotMsgSource.DATA);
    }

    @Test
    void givenMsgFromMetadata_whenOnMsg_thenVerifyOutput() throws Exception {
        node.onMsg(ctx, getJnksIotMsg(deviceId, JnksIotMsg.EMPTY_JSON_OBJECT));

        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx).tellSuccess(newMsgCaptor.capture());
        verify(ctx, never()).tellFailure(any(), any());

        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();

        JsonNode dataNode = JacksonUtil.toJsonNode(newMsg.getData());
        assertThat(dataNode.has("TestKey_1")).isEqualTo(true);
        assertThat(dataNode.has("voltageDataValue")).isEqualTo(true);
    }

    @Test
    void givenMsgFromMsg_whenOnMsg_thenVerifyOutput() throws Exception {
        config.setCopyFrom(JnksIotMsgSource.DATA);
        config.setKeys(Set.of(".*Key$"));
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        node.init(ctx, nodeConfiguration);

        String data = "{\"nullKey\":null,\"stringKey\":\"value1\",\"booleanKey\":true,\"doubleKey\":42.0,\"longKey\":73," +
                "\"jsonKey\":{\"someNumber\":42,\"someArray\":[1,2,3],\"someNestedObject\":{\"key\":\"value\"}}}";
        node.onMsg(ctx, getJnksIotMsg(deviceId, data));

        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx).tellSuccess(newMsgCaptor.capture());
        verify(ctx, never()).tellFailure(any(), any());

        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();

        Map<String, String> metaDataMap = newMsg.getMetaData().getData();
        assertThat(metaDataMap.get("nullKey")).isEqualTo("null");
        assertThat(metaDataMap.get("stringKey")).isEqualTo("value1");
        assertThat(metaDataMap.get("booleanKey")).isEqualTo("true");
        assertThat(metaDataMap.get("doubleKey")).isEqualTo("42.0");
        assertThat(metaDataMap.get("longKey")).isEqualTo("73");
        assertThat(metaDataMap.get("jsonKey"))
                .isEqualTo("{\"someNumber\":42,\"someArray\":[1,2,3],\"someNestedObject\":{\"key\":\"value\"}}");
    }

    @Test
    void givenEmptyKeys_whenOnMsg_thenVerifyOutput() throws Exception {
        JnksIotCopyKeysNodeConfiguration defaultConfig = new JnksIotCopyKeysNodeConfiguration().defaultConfiguration();
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(defaultConfig));
        node.init(ctx, nodeConfiguration);

        String data = "{\"DigitData\":22.5,\"TempDataValue\":10.5}";
        JnksIotMsg msg = getJnksIotMsg(deviceId, data);
        node.onMsg(ctx, msg);

        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx).tellSuccess(newMsgCaptor.capture());
        verify(ctx, never()).tellFailure(any(), any());

        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();

        assertThat(newMsg.getMetaData()).isEqualTo(msg.getMetaData());
    }

    @Test
    void givenMsgDataNotJSONObject_whenOnMsg_thenTVerifyOutput() throws Exception {
        JnksIotMsg msg = getJnksIotMsg(deviceId, JnksIotMsg.EMPTY_JSON_ARRAY);
        node.onMsg(ctx, msg);

        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx).tellSuccess(newMsgCaptor.capture());
        verify(ctx, never()).tellFailure(any(), any());

        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();

        assertThat(newMsg).isSameAs(msg);
    }

    private static Stream<Arguments> givenFromVersionAndConfig_whenUpgrade_thenVerifyUpgradeResultAndConfig() {
        return Stream.of(
                Arguments.of(0, "{\"fromMetadata\":false,\"keys\":[\"temperature\"]}", true, "{\"copyFrom\":\"DATA\",\"keys\":[\"temperature\"]}"),
                Arguments.of(0, "{\"fromMetadata\":true,\"keys\":[\"temperature\"]}", true, "{\"copyFrom\":\"METADATA\",\"keys\":[\"temperature\"]}"),
                Arguments.of(1, "{\"fromMetadata\":\"METADATA\",\"keys\":[\"temperature\"]}", true, "{\"copyFrom\":\"METADATA\",\"keys\":[\"temperature\"]}"),
                Arguments.of(1, "{\"fromMetadata\":\"DATA\",\"keys\":[\"temperature\"]}", true, "{\"copyFrom\":\"DATA\",\"keys\":[\"temperature\"]}"),
                Arguments.of(1, "{\"copyFrom\":\"METADATA\",\"keys\":[\"temperature\"]}", false, "{\"copyFrom\":\"METADATA\",\"keys\":[\"temperature\"]}"),
                Arguments.of(1, "{\"copyFrom\":\"DATA\",\"keys\":[\"temperature\"]}", false, "{\"copyFrom\":\"DATA\",\"keys\":[\"temperature\"]}")
        );
    }

    @ParameterizedTest
    @MethodSource
    void givenFromVersionAndConfig_whenUpgrade_thenVerifyUpgradeResultAndConfig(int givenVersion, String givenConfigStr,
                                                                                boolean hasChanges, String expectedConfigStr) throws Exception {
        // GIVEN
        JsonNode givenConfig = JacksonUtil.toJsonNode(givenConfigStr);
        JsonNode expectedConfig = JacksonUtil.toJsonNode(expectedConfigStr);

        // WHEN
        var upgradeResult = node.upgrade(givenVersion, givenConfig);

        // THEN
        assertThat(upgradeResult.getFirst()).isEqualTo(hasChanges);
        ObjectNode upgradedConfig = (ObjectNode) upgradeResult.getSecond();
        assertThat(upgradedConfig).isEqualTo(expectedConfig);
    }

    private JnksIotMsg getJnksIotMsg(EntityId entityId, String data) {
        final Map<String, String> mdMap = Map.of(
                "TestKey_1", "Test",
                "country", "US",
                "voltageDataValue", "220",
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
