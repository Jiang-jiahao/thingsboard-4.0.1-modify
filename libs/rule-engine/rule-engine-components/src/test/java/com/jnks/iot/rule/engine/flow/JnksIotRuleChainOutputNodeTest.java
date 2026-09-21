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
import com.jnks.iot.server.common.data.rule.RuleNode;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.spy;
import static org.mockito.BDDMockito.then;

@ExtendWith(MockitoExtension.class)
public class JnksIotRuleChainOutputNodeTest {

    private JnksIotRuleChainOutputNode node;
    private EmptyNodeConfiguration config;
    private JnksIotNodeConfiguration nodeConfiguration;

    @Mock
    private JnksIotContext ctxMock;

    @BeforeEach
    public void setUp() {
        node = spy(new JnksIotRuleChainOutputNode());
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
    public void givenRuleNodeName_whenOnMsg_thenForwardMsgToTheCallerRuleChainWithRelationTypeMatchesWithRuleNodeName() throws JnksIotNodeException {
        RuleNode ruleNode = new RuleNode();
        ruleNode.setName("test");
        given(ctxMock.getSelf()).willReturn(ruleNode);

        node.init(ctxMock, nodeConfiguration);
        DeviceId deviceId = new DeviceId(UUID.fromString("f514da88-79b3-46da-9f02-1747c5e84f44"));
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(deviceId)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();
        node.onMsg(ctxMock, msg);

        then(ctxMock).should().output(msg, "test");
    }

}
