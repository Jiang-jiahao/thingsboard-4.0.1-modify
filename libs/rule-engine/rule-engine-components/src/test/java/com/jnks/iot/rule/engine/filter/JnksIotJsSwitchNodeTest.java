package com.jnks.iot.rule.engine.filter;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class JnksIotJsSwitchNodeTest {

    private JnksIotJsSwitchNode node;

    @Mock
    private JnksIotContext ctx;
    @Mock
    private ScriptEngine scriptEngine;

    private final RuleChainId ruleChainId = new RuleChainId(Uuids.timeBased());
    private final RuleNodeId ruleNodeId = new RuleNodeId(Uuids.timeBased());

    @Test
    public void multipleRoutesAreAllowed() throws JnksIotNodeException {
        initWithScript();
        JnksIotMsgMetaData metaData = new JnksIotMsgMetaData();
        metaData.putValue("temp", "10");
        metaData.putValue("humidity", "99");
        String rawJson = "{\"name\": \"Vit\", \"passed\": 5}";

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(null)
                .copyMetaData(metaData)
                .dataType(JnksIotMsgDataType.JSON)
                .data(rawJson)
                .ruleChainId(ruleChainId)
                .ruleNodeId(ruleNodeId)
                .build();
        when(scriptEngine.executeSwitchAsync(msg)).thenReturn(Futures.immediateFuture(Sets.newHashSet("one", "three")));

        node.onMsg(ctx, msg);
        verify(ctx).tellNext(msg, Sets.newHashSet("one", "three"));
    }

    private void initWithScript() throws JnksIotNodeException {
        JnksIotJsSwitchNodeConfiguration config = new JnksIotJsSwitchNodeConfiguration();
        config.setScriptLang(ScriptLanguage.JS);
        config.setJsScript("scr");
        JnksIotNodeConfiguration nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        when(ctx.createScriptEngine(ScriptLanguage.JS, "scr")).thenReturn(scriptEngine);

        node = new JnksIotJsSwitchNode();
        node.init(ctx, nodeConfiguration);
    }
}
