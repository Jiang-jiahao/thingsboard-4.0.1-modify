package com.jnks.iot.rule.engine.filter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.id.AssetId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class JnksIotOriginatorTypeFilterNodeTest {

    private JnksIotContext ctx;
    private JnksIotOriginatorTypeFilterNode node;

    @BeforeEach
    void setUp() throws JnksIotNodeException {
        ctx = mock(JnksIotContext.class);
        var config = new JnksIotOriginatorTypeFilterNodeConfiguration().defaultConfiguration();
        node = new JnksIotOriginatorTypeFilterNode();
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));
    }

    @AfterEach
    void tearDown() {
        node.destroy();
    }

    @Test
    void givenDevice_whenOnMsg_then_True() {
        // GIVEN
        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        JnksIotMsg msg = getJnksIotMsg(deviceId);

        // WHEN
        node.onMsg(ctx, msg);

        // THEN
        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.TRUE));
        verify(ctx, never()).tellFailure(any(), any());
        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();
        assertThat(newMsg).isSameAs(msg);
    }

    @Test
    void givenAsset_whenOnMsg_then_False() {
        // GIVEN
        AssetId assetId = new AssetId(UUID.randomUUID());
        JnksIotMsg msg = getJnksIotMsg(assetId);

        // WHEN
        node.onMsg(ctx, msg);

        // THEN
        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.FALSE));
        verify(ctx, never()).tellFailure(any(), any());
        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();
        assertThat(newMsg).isSameAs(msg);
    }

    private JnksIotMsg getJnksIotMsg(EntityId entityId) {
        return JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_ATTRIBUTES_REQUEST)
                .originator(entityId)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();
    }

}
