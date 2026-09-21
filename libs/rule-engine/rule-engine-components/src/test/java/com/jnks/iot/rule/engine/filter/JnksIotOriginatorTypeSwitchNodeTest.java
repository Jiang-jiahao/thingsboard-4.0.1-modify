package com.jnks.iot.rule.engine.filter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.EntityIdFactory;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class JnksIotOriginatorTypeSwitchNodeTest {

    private static final UUID RANDOM_UUID = UUID.randomUUID();

    private JnksIotOriginatorTypeSwitchNode node;

    private JnksIotContext ctx;

    @BeforeEach
    void setUp() {
        ctx = mock(JnksIotContext.class);
        node = new JnksIotOriginatorTypeSwitchNode();
    }

    @AfterEach
    void tearDown() {
        node.destroy();
    }

    @Test
    void givenAllTypes_whenOnMsg_then_allTypesSupported() throws JnksIotNodeException {
        // GIVEN
        List<JnksIotMsg> jnksIotMsgList = new ArrayList<>();
        var entityTypes = EntityType.values();
        for (var entityType : entityTypes) {
            var entityId = EntityIdFactory.getByTypeAndUuid(entityType, RANDOM_UUID);
            jnksIotMsgList.add(getJnksIotMsg(entityId));
        }

        // WHEN
        for (JnksIotMsg jnksIotMsg : jnksIotMsgList) {
            node.onMsg(ctx, jnksIotMsg);
        }

        // THEN
        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        ArgumentCaptor<String> nodeConnectionCapture = ArgumentCaptor.forClass(String.class);
        verify(ctx, times(jnksIotMsgList.size())).tellNext(newMsgCaptor.capture(), nodeConnectionCapture.capture());
        verify(ctx, never()).tellFailure(any(), any());
        var resultMsgs = newMsgCaptor.getAllValues();
        var resultNodeConnections = nodeConnectionCapture.getAllValues();
        for (int i = 0; i < resultMsgs.size(); i++) {
            var msg = resultMsgs.get(i);
            assertThat(msg).isNotNull();
            assertThat(msg).isSameAs(jnksIotMsgList.get(i));
            assertThat(resultNodeConnections.get(i))
                    .isEqualTo(msg.getOriginator().getEntityType().getNormalName());
        }
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
