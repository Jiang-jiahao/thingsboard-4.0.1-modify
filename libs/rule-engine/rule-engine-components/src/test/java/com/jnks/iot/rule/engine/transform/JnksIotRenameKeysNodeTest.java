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

import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class JnksIotRenameKeysNodeTest {
    DeviceId deviceId;
    JnksIotRenameKeysNode node;
    JnksIotRenameKeysNodeConfiguration config;
    JnksIotNodeConfiguration nodeConfiguration;
    JnksIotContext ctx;
    JnksIotMsgCallback callback;

    @BeforeEach
    void setUp() throws JnksIotNodeException {
        deviceId = new DeviceId(UUID.randomUUID());
        callback = mock(JnksIotMsgCallback.class);
        ctx = mock(JnksIotContext.class);
        config = new JnksIotRenameKeysNodeConfiguration().defaultConfiguration();
        config.setRenameKeysMapping(Map.of("TestKey_1", "Attribute_1", "TestKey_2", "Attribute_2"));
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        node = spy(new JnksIotRenameKeysNode());
        node.init(ctx, nodeConfiguration);
    }

    @AfterEach
    void tearDown() {
        node.destroy();
    }

    @Test
    void givenDefaultConfig_whenVerify_thenOK() {
        JnksIotRenameKeysNodeConfiguration defaultConfig = new JnksIotRenameKeysNodeConfiguration().defaultConfiguration();
        assertThat(defaultConfig.getRenameKeysMapping()).isEqualTo(Map.of("temperatureCelsius", "temperature"));
        assertThat(defaultConfig.getRenameIn()).isEqualTo(JnksIotMsgSource.DATA);
    }

    @Test
    void givenMsg_whenOnMsg_thenVerifyOutput() throws Exception {
        String data = "{\"Temperature_1\":22.5,\"TestKey_2\":10.3}";
        node.onMsg(ctx, getJnksIotMsg(deviceId, data));

        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellSuccess(newMsgCaptor.capture());
        verify(ctx, never()).tellFailure(any(), any());

        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();

        JsonNode dataNode = JacksonUtil.toJsonNode(newMsg.getData());
        assertThat(dataNode.has("Attribute_2")).isEqualTo(true);
        assertThat(dataNode.has("Temperature_1")).isEqualTo(true);
    }

    @Test
    void givenMetadata_whenOnMsg_thenVerifyOutput() throws Exception {
        config = new JnksIotRenameKeysNodeConfiguration().defaultConfiguration();
        config.setRenameKeysMapping(Map.of("TestKey_1", "Attribute_1", "TestKey_2", "Attribute_2"));
        config.setRenameIn(JnksIotMsgSource.METADATA);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        node.init(ctx, nodeConfiguration);

        String data = "{\"Temperature_1\":22.5,\"TestKey_2\":10.3}";
        node.onMsg(ctx, getJnksIotMsg(deviceId, data));

        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellSuccess(newMsgCaptor.capture());
        verify(ctx, never()).tellFailure(any(), any());

        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();

        Map<String, String> mdDataMap = newMsg.getMetaData().getData();
        assertThat(mdDataMap.containsKey("Attribute_1")).isEqualTo(true);
    }

    @Test
    void givenEmptyKeys_whenOnMsg_thenVerifyOutput() throws Exception {
        JnksIotRenameKeysNodeConfiguration defaultConfig = new JnksIotRenameKeysNodeConfiguration().defaultConfiguration();
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(defaultConfig));
        node.init(ctx, nodeConfiguration);

        String data = "{\"Temperature_1\":22.5,\"TestKey_2\":10.3}";
        JnksIotMsg msg = getJnksIotMsg(deviceId, data);
        node.onMsg(ctx, msg);

        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellSuccess(newMsgCaptor.capture());
        verify(ctx, never()).tellFailure(any(), any());

        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();

        assertThat(newMsg.getMetaData()).isEqualTo(msg.getMetaData());
    }

    @Test
    void givenMsgDataNotJSONObject_whenOnMsg_thenVerifyOutput() throws Exception {
        JnksIotMsg msg = getJnksIotMsg(deviceId, JnksIotMsg.EMPTY_JSON_ARRAY);
        node.onMsg(ctx, msg);

        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellSuccess(newMsgCaptor.capture());
        verify(ctx, never()).tellFailure(any(), any());

        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();

        assertThat(newMsg).isSameAs(msg);
    }

    private static Stream<Arguments> givenFromVersionAndConfig_whenUpgrade_thenVerifyUpgradeResultAndConfig() {
        return Stream.of(
                Arguments.of(0, "{\"fromMetadata\":false,\"renameKeysMapping\":{\"temp\":\"temperature\"}}", true, "{\"renameIn\":\"DATA\",\"renameKeysMapping\":{\"temp\":\"temperature\"}}"),
                Arguments.of(0, "{\"fromMetadata\":true,\"renameKeysMapping\":{\"temp\":\"temperature\"}}", true, "{\"renameIn\":\"METADATA\",\"renameKeysMapping\":{\"temp\":\"temperature\"}}"),
                Arguments.of(1, "{\"fromMetadata\":\"METADATA\",\"renameKeysMapping\":{\"temp\":\"temperature\"}}", true, "{\"renameIn\":\"METADATA\",\"renameKeysMapping\":{\"temp\":\"temperature\"}}"),
                Arguments.of(1, "{\"fromMetadata\":\"DATA\",\"renameKeysMapping\":{\"temp\":\"temperature\"}}", true, "{\"renameIn\":\"DATA\",\"renameKeysMapping\":{\"temp\":\"temperature\"}}"),
                Arguments.of(1, "{\"renameIn\":\"METADATA\",\"renameKeysMapping\":{\"temp\":\"temperature\"}}", false, "{\"renameIn\":\"METADATA\",\"renameKeysMapping\":{\"temp\":\"temperature\"}}"),
                Arguments.of(1, "{\"renameIn\":\"DATA\",\"renameKeysMapping\":{\"temp\":\"temperature\"}}", false, "{\"renameIn\":\"DATA\",\"renameKeysMapping\":{\"temp\":\"temperature\"}}")
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
