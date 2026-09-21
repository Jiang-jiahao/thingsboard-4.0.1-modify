package com.jnks.iot.rule.engine.flow;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.EmptyNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.BDDMockito.then;

@ExtendWith(MockitoExtension.class)
public class JnksIotAckNodeTest {

    private JnksIotAckNode node;
    private EmptyNodeConfiguration config;
    private JnksIotNodeConfiguration nodeConfiguration;

    @Mock
    private JnksIotContext ctxMock;

    @BeforeEach
    public void setUp() {
        node = new JnksIotAckNode();
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

    @Test
    public void givenMsg_whenOnMsg_thenAckAndTellSuccess() throws JnksIotNodeException {
        node.init(ctxMock, nodeConfiguration);
        DeviceId deviceId = new DeviceId(UUID.fromString("5770153d-6ca2-4447-8a54-5d8a4538e052"));
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(deviceId)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();
        node.onMsg(ctxMock, msg);

        then(ctxMock).should().ack(msg);
        then(ctxMock).should().tellSuccess(msg);
    }

}
