package com.jnks.iot.rule.engine.transform;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.google.common.util.concurrent.Futures;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.ScriptEngine;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.id.RuleNodeId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.script.ScriptLanguage;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgDataType;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class JnksIotTransformMsgNodeTest {

    private JnksIotTransformMsgNode node;

    @Mock
    private JnksIotContext ctx;
    @Mock
    private ScriptEngine scriptEngine;

    @Test
    public void metadataCanBeUpdated() throws JnksIotNodeException {
        initWithScript();
        JnksIotMsgMetaData metaData = new JnksIotMsgMetaData();
        metaData.putValue("temp", "7");
        String rawJson = "{\"passed\": 5}";

        RuleChainId ruleChainId = new RuleChainId(Uuids.timeBased());
        RuleNodeId ruleNodeId = new RuleNodeId(Uuids.timeBased());
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(null)
                .copyMetaData(metaData)
                .dataType(JnksIotMsgDataType.JSON)
                .data(rawJson)
                .ruleChainId(ruleChainId)
                .ruleNodeId(ruleNodeId)
                .build();
        JnksIotMsg transformedMsg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(null)
                .copyMetaData(metaData)
                .dataType(JnksIotMsgDataType.JSON)
                .data("{new}")
                .ruleChainId(ruleChainId)
                .ruleNodeId(ruleNodeId)
                .build();
        when(scriptEngine.executeUpdateAsync(msg)).thenReturn(Futures.immediateFuture(Collections.singletonList(transformedMsg)));

        node.onMsg(ctx, msg);
        ArgumentCaptor<JnksIotMsg> captor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx).tellSuccess(captor.capture());
        JnksIotMsg actualMsg = captor.getValue();
        assertEquals(transformedMsg, actualMsg);
    }

    @Test
    public void exceptionHandledCorrectly() throws JnksIotNodeException {
        initWithScript();
        JnksIotMsgMetaData metaData = new JnksIotMsgMetaData();
        metaData.putValue("temp", "7");
        String rawJson = "{\"passed\": 5";

        RuleChainId ruleChainId = new RuleChainId(Uuids.timeBased());
        RuleNodeId ruleNodeId = new RuleNodeId(Uuids.timeBased());
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(null)
                .copyMetaData(metaData)
                .dataType(JnksIotMsgDataType.JSON)
                .data(rawJson)
                .ruleChainId(ruleChainId)
                .ruleNodeId(ruleNodeId)
                .build();
        when(scriptEngine.executeUpdateAsync(msg)).thenReturn(Futures.immediateFailedFuture(new IllegalStateException("error")));

        node.onMsg(ctx, msg);
        verifyError(msg, "error", IllegalStateException.class);
    }

    private void initWithScript() throws JnksIotNodeException {
        JnksIotTransformMsgNodeConfiguration config = new JnksIotTransformMsgNodeConfiguration();
        config.setScriptLang(ScriptLanguage.JS);
        config.setJsScript("scr");
        JnksIotNodeConfiguration nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        when(ctx.createScriptEngine(ScriptLanguage.JS, "scr")).thenReturn(scriptEngine);

        node = new JnksIotTransformMsgNode();
        node.init(ctx, nodeConfiguration);
    }

    private void verifyError(JnksIotMsg msg, String message, Class expectedClass) {
        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(ctx).tellFailure(same(msg), captor.capture());

        Throwable value = captor.getValue();
        assertEquals(expectedClass, value.getClass());
        assertEquals(message, value.getMessage());
    }
}
