package com.jnks.iot.rule.engine.filter;

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
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.data.script.ScriptLanguage;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgDataType;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import javax.script.ScriptException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class JnksIotJsFilterNodeTest {

    private JnksIotJsFilterNode node;

    @Mock
    private JnksIotContext ctx;
    @Mock
    private ScriptEngine scriptEngine;

    private final RuleChainId ruleChainId = new RuleChainId(Uuids.timeBased());
    private final RuleNodeId ruleNodeId = new RuleNodeId(Uuids.timeBased());

    @Test
    public void falseEvaluationDoNotSendMsg() throws JnksIotNodeException {
        initWithScript();
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(null)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .dataType(JnksIotMsgDataType.JSON)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .ruleChainId(ruleChainId)
                .ruleNodeId(ruleNodeId)
                .build();
        when(scriptEngine.executeFilterAsync(msg)).thenReturn(Futures.immediateFuture(false));

        node.onMsg(ctx, msg);
        verify(ctx).getDbCallbackExecutor();
        verify(ctx).tellNext(msg, JnksIotNodeConnectionType.FALSE);
    }

    @Test
    public void exceptionInJsThrowsException() throws JnksIotNodeException {
        initWithScript();
        JnksIotMsgMetaData metaData = new JnksIotMsgMetaData();
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(null)
                .copyMetaData(metaData)
                .dataType(JnksIotMsgDataType.JSON)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .ruleChainId(ruleChainId)
                .ruleNodeId(ruleNodeId)
                .build();
        when(scriptEngine.executeFilterAsync(msg)).thenReturn(Futures.immediateFailedFuture(new ScriptException("error")));


        node.onMsg(ctx, msg);
        verifyError(msg, "error", ScriptException.class);
    }

    @Test
    public void metadataConditionCanBeTrue() throws JnksIotNodeException {
        initWithScript();
        JnksIotMsgMetaData metaData = new JnksIotMsgMetaData();
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(null)
                .copyMetaData(metaData)
                .dataType(JnksIotMsgDataType.JSON)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .ruleChainId(ruleChainId)
                .ruleNodeId(ruleNodeId)
                .build();
        when(scriptEngine.executeFilterAsync(msg)).thenReturn(Futures.immediateFuture(true));

        node.onMsg(ctx, msg);
        verify(ctx).getDbCallbackExecutor();
        verify(ctx).tellNext(msg, JnksIotNodeConnectionType.TRUE);
    }

    private void initWithScript() throws JnksIotNodeException {
        JnksIotJsFilterNodeConfiguration config = new JnksIotJsFilterNodeConfiguration();
        config.setScriptLang(ScriptLanguage.JS);
        config.setJsScript("scr");
        JnksIotNodeConfiguration nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        when(ctx.createScriptEngine(ScriptLanguage.JS, "scr")).thenReturn(scriptEngine);

        node = new JnksIotJsFilterNode();
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
