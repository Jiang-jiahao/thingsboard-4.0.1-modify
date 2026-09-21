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

public class JnksIotDeleteKeysNodeTest {
    DeviceId deviceId;
    JnksIotDeleteKeysNode node;
    JnksIotDeleteKeysNodeConfiguration config;
    JnksIotNodeConfiguration nodeConfiguration;
    JnksIotContext ctx;
    JnksIotMsgCallback callback;

    @BeforeEach
    void setUp() throws JnksIotNodeException {
        deviceId = new DeviceId(UUID.randomUUID());
        callback = mock(JnksIotMsgCallback.class);
        ctx = mock(JnksIotContext.class);
        config = new JnksIotDeleteKeysNodeConfiguration().defaultConfiguration();
        config.setKeys(Set.of("TestKey_1", "TestKey_2", "TestKey_3", "(\\w*)Data(\\w*)"));
        config.setDeleteFrom(JnksIotMsgSource.METADATA);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        node = spy(new JnksIotDeleteKeysNode());
        node.init(ctx, nodeConfiguration);
    }

    @AfterEach
    void tearDown() {
        node.destroy();
    }

    @Test
    void givenDefaultConfig_whenVerify_thenOK() {
        JnksIotDeleteKeysNodeConfiguration defaultConfig = new JnksIotDeleteKeysNodeConfiguration().defaultConfiguration();
        assertThat(defaultConfig.getKeys()).isEqualTo(Collections.emptySet());
        assertThat(defaultConfig.getDeleteFrom()).isEqualTo(JnksIotMsgSource.DATA);
    }

    @Test
    void givenDeleteFromMetadata_whenOnMsg_thenVerifyOutput() throws Exception {
        node.onMsg(ctx, getJnksIotMsg(deviceId, JnksIotMsg.EMPTY_JSON_OBJECT));

        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx).tellSuccess(newMsgCaptor.capture());
        verify(ctx, never()).tellFailure(any(), any());

        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();

        Map<String, String> metaDataMap = newMsg.getMetaData().getData();
        assertThat(metaDataMap.containsKey("TestKey_1")).isEqualTo(false);
        assertThat(metaDataMap.containsKey("voltageDataValue")).isEqualTo(false);
    }

    @Test
    void givenDeleteFromMsgConfig_whenOnMsg_thenVerifyOutput() throws Exception {
        config.setDeleteFrom(JnksIotMsgSource.DATA);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        node.init(ctx, nodeConfiguration);

        String data = "{\"Voltage\":22.5,\"TempDataValue\":10.5}";
        node.onMsg(ctx, getJnksIotMsg(deviceId, data));

        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx).tellSuccess(newMsgCaptor.capture());
        verify(ctx, never()).tellFailure(any(), any());

        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();

        JsonNode dataNode = JacksonUtil.toJsonNode(newMsg.getData());
        assertThat(dataNode.has("TempDataValue")).isEqualTo(false);
        assertThat(dataNode.has("Voltage")).isEqualTo(true);
    }

    @Test
    void givenEmptyKeys_whenOnMsg_thenVerifyOutput() throws Exception {
        JnksIotDeleteKeysNodeConfiguration defaultConfig = new JnksIotDeleteKeysNodeConfiguration().defaultConfiguration();
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(defaultConfig));
        node.init(ctx, nodeConfiguration);

        String data = "{\"Voltage\":220,\"Humidity\":56}";
        node.onMsg(ctx, getJnksIotMsg(deviceId, data));

        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx).tellSuccess(newMsgCaptor.capture());
        verify(ctx, never()).tellFailure(any(), any());

        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();

        assertThat(newMsg.getData()).isEqualTo(data);
    }

    private static Stream<Arguments> givenFromVersionAndConfig_whenUpgrade_thenVerifyUpgradeResultAndConfig() {
        return Stream.of(
                Arguments.of(0, "{\"fromMetadata\":false,\"keys\":[\"temperature\"]}", true, "{\"deleteFrom\":\"DATA\",\"keys\":[\"temperature\"]}"),
                Arguments.of(0, "{\"fromMetadata\":true,\"keys\":[\"temperature\"]}", true, "{\"deleteFrom\":\"METADATA\",\"keys\":[\"temperature\"]}"),
                Arguments.of(1, "{\"dataToFetch\":\"METADATA\",\"keys\":[\"temperature\"]}", true, "{\"deleteFrom\":\"METADATA\",\"keys\":[\"temperature\"]}"),
                Arguments.of(1, "{\"dataToFetch\":\"DATA\",\"keys\":[\"temperature\"]}", true, "{\"deleteFrom\":\"DATA\",\"keys\":[\"temperature\"]}"),
                Arguments.of(1, "{\"deleteFrom\":\"METADATA\",\"keys\":[\"temperature\"]}", false, "{\"deleteFrom\":\"METADATA\",\"keys\":[\"temperature\"]}"),
                Arguments.of(1, "{\"deleteFrom\":\"DATA\",\"keys\":[\"temperature\"]}", false, "{\"deleteFrom\":\"DATA\",\"keys\":[\"temperature\"]}")
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
