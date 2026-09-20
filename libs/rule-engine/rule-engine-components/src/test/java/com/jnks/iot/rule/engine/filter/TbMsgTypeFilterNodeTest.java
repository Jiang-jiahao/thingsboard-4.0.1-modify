package com.jnks.iot.rule.engine.filter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.TbContext;
import com.jnks.iot.rule.engine.api.TbNodeConfiguration;
import com.jnks.iot.rule.engine.api.TbNodeException;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.msg.TbMsgType;
import com.jnks.iot.server.common.data.msg.TbNodeConnectionType;
import com.jnks.iot.server.common.msg.TbMsg;
import com.jnks.iot.server.common.msg.TbMsgMetaData;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static com.jnks.iot.server.common.data.msg.TbMsgType.ATTRIBUTES_UPDATED;
import static com.jnks.iot.server.common.data.msg.TbMsgType.POST_ATTRIBUTES_REQUEST;

class TbMsgTypeFilterNodeTest {

    private DeviceId deviceId;
    private TbContext ctx;
    private TbMsgTypeFilterNode node;

    @BeforeEach
    void setUp() throws TbNodeException {
        ctx = mock(TbContext.class);
        var config = new TbMsgTypeFilterNodeConfiguration().defaultConfiguration();
        deviceId = new DeviceId(UUID.randomUUID());
        node = new TbMsgTypeFilterNode();
        node.init(ctx, new TbNodeConfiguration(JacksonUtil.valueToTree(config)));
    }

    @AfterEach
    void tearDown() {
        node.destroy();
    }

    @Test
    void givenPostAttributes_whenOnMsg_then_True() {
        // GIVEN
        TbMsg msg = getTbMsg(deviceId, POST_ATTRIBUTES_REQUEST);

        // WHEN
        node.onMsg(ctx, msg);

        // THEN
        ArgumentCaptor<TbMsg> newMsgCaptor = ArgumentCaptor.forClass(TbMsg.class);
        verify(ctx, times(1)).tellNext(newMsgCaptor.capture(), eq(TbNodeConnectionType.TRUE));
        verify(ctx, never()).tellFailure(any(), any());
        TbMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();
        assertThat(newMsg).isSameAs(msg);
    }

    @Test
    void givenAttributesUpdated_whenOnMsg_then_False() {
        // GIVEN
        TbMsg msg = getTbMsg(deviceId, ATTRIBUTES_UPDATED);

        // WHEN
        node.onMsg(ctx, msg);

        // THEN
        ArgumentCaptor<TbMsg> newMsgCaptor = ArgumentCaptor.forClass(TbMsg.class);
        verify(ctx, times(1)).tellNext(newMsgCaptor.capture(), eq(TbNodeConnectionType.FALSE));
        verify(ctx, never()).tellFailure(any(), any());
        TbMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();
        assertThat(newMsg).isSameAs(msg);
    }

    private TbMsg getTbMsg(EntityId entityId, TbMsgType msgType) {
        return TbMsg.newMsg()
                .type(msgType)
                .originator(entityId)
                .copyMetaData(TbMsgMetaData.EMPTY)
                .data(TbMsg.EMPTY_JSON_OBJECT)
                .build();
    }

}
