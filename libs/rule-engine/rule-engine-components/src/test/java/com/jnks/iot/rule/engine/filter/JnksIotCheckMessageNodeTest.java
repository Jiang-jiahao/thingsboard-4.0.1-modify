package com.jnks.iot.rule.engine.filter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.DataConstants;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class JnksIotCheckMessageNodeTest {

    private static final DeviceId DEVICE_ID = new DeviceId(UUID.randomUUID());
    private static final JnksIotMsg EMPTY_POST_ATTRIBUTES_MSG = JnksIotMsg.newMsg()
            .type(JnksIotMsgType.POST_ATTRIBUTES_REQUEST)
            .originator(DEVICE_ID)
            .copyMetaData(JnksIotMsgMetaData.EMPTY)
            .data(JnksIotMsg.EMPTY_JSON_OBJECT)
            .build();

    private JnksIotCheckMessageNode node;

    private JnksIotContext ctx;

    @BeforeEach
    void setUp() {
        ctx = mock(JnksIotContext.class);
        node = new JnksIotCheckMessageNode();
    }

    @AfterEach
    void tearDown() {
        node.destroy();
    }

    @Test
    void givenDefaultConfig_whenOnMsg_then_True() throws JnksIotNodeException {
        // GIVEN
        var configuration = new JnksIotCheckMessageNodeConfiguration().defaultConfiguration();
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(configuration)));

        // WHEN
        node.onMsg(ctx, EMPTY_POST_ATTRIBUTES_MSG);

        // THEN
        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.TRUE));
        verify(ctx, never()).tellFailure(any(), any());
        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();
        assertThat(newMsg).isSameAs(EMPTY_POST_ATTRIBUTES_MSG);
    }

    @Test
    void givenCustomConfigWithoutCheckAllKeysAndWithEmptyLists_whenOnMsg_then_False() throws JnksIotNodeException {
        // GIVEN
        var configuration = new JnksIotCheckMessageNodeConfiguration().defaultConfiguration();
        configuration.setCheckAllKeys(false);
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(configuration)));

        // WHEN
        node.onMsg(ctx, EMPTY_POST_ATTRIBUTES_MSG);

        // THEN
        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.FALSE));
        verify(ctx, never()).tellFailure(any(), any());
        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();
        assertThat(newMsg).isSameAs(EMPTY_POST_ATTRIBUTES_MSG);
    }

    @Test
    void givenCustomConfigWithCheckAllKeys_whenOnMsg_then_True() throws JnksIotNodeException {
        // GIVEN
        var configuration = new JnksIotCheckMessageNodeConfiguration().defaultConfiguration();
        configuration.setMessageNames(List.of("temperature-0"));
        configuration.setMetadataNames(List.of("deviceName", "deviceType", "ts"));
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(configuration)));

        JnksIotMsg jnksIotMsg = getJnksIotMsg();

        // WHEN
        node.onMsg(ctx, jnksIotMsg);

        // THEN
        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.TRUE));
        verify(ctx, never()).tellFailure(any(), any());
        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();
        assertThat(newMsg).isSameAs(jnksIotMsg);
    }

    @Test
    void givenCustomConfigWithCheckAllKeys_whenOnMsg_then_False() throws JnksIotNodeException {
        // GIVEN
        var configuration = new JnksIotCheckMessageNodeConfiguration().defaultConfiguration();
        configuration.setMessageNames(List.of("temperature-0", "temperature-1"));
        configuration.setMetadataNames(List.of("deviceName", "deviceType", "ts"));
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(configuration)));

        JnksIotMsg jnksIotMsg = getJnksIotMsg();

        // WHEN
        node.onMsg(ctx, jnksIotMsg);

        // THEN
        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.FALSE));
        verify(ctx, never()).tellFailure(any(), any());
        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();
        assertThat(newMsg).isSameAs(jnksIotMsg);
    }

    @Test
    void givenCustomConfigWithoutCheckAllKeys_whenOnMsg_then_True() throws JnksIotNodeException {
        // GIVEN
        var configuration = new JnksIotCheckMessageNodeConfiguration().defaultConfiguration();
        configuration.setMessageNames(List.of("temperature-0", "temperature-1"));
        configuration.setCheckAllKeys(false);
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(configuration)));

        JnksIotMsg jnksIotMsg = getJnksIotMsg();

        // WHEN
        node.onMsg(ctx, jnksIotMsg);

        // THEN
        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.TRUE));
        verify(ctx, never()).tellFailure(any(), any());
        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();
        assertThat(newMsg).isSameAs(jnksIotMsg);
    }

    @Test
    void givenCustomConfigWithoutCheckAllKeysAndEmptyMsg_whenOnMsg_then_False() throws JnksIotNodeException {
        // GIVEN
        var configuration = new JnksIotCheckMessageNodeConfiguration().defaultConfiguration();
        configuration.setMessageNames(List.of("temperature-0", "temperature-1"));
        configuration.setCheckAllKeys(false);
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(configuration)));

        JnksIotMsg jnksIotMsg = getJnksIotMsg(true);

        // WHEN
        node.onMsg(ctx, jnksIotMsg);

        // THEN
        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.FALSE));
        verify(ctx, never()).tellFailure(any(), any());
        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();
        assertThat(newMsg).isSameAs(jnksIotMsg);
    }

    private JnksIotMsg getJnksIotMsg() {
        return getJnksIotMsg(false);
    }

    private JnksIotMsg getJnksIotMsg(boolean emptyData) {
        String data = emptyData ? JnksIotMsg.EMPTY_JSON_OBJECT : "{\"temperature-0\": 25}";
        var metadata = new JnksIotMsgMetaData();
        metadata.putValue(DataConstants.DEVICE_NAME, "Test Device");
        metadata.putValue(DataConstants.DEVICE_TYPE, DataConstants.DEFAULT_DEVICE_TYPE);
        metadata.putValue("ts", String.valueOf(System.currentTimeMillis()));
        return JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_ATTRIBUTES_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(metadata)
                .data(data)
                .build();
    }

}
