package com.jnks.iot.rule.engine.filter;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.ScriptEngine;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.script.ScriptLanguage;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import static com.jnks.iot.common.util.DonAsynchron.withCallback;

@Slf4j
@RuleNode(
        type = ComponentType.FILTER,
        name = "script",
        relationTypes = {JnksIotNodeConnectionType.TRUE, JnksIotNodeConnectionType.FALSE},
        configClazz = JnksIotJsFilterNodeConfiguration.class,
        nodeDescription = "Filter incoming messages using TBEL or JS script",
        nodeDetails = "Evaluates boolean function using incoming message. " +
                "The function may be written using TBEL or plain JavaScript. " +
                "Script function should return boolean value and accepts three parameters: <br/>" +
                "Message payload can be accessed via <code>msg</code> property. For example <code>msg.temperature < 10;</code><br/>" +
                "Message metadata can be accessed via <code>metadata</code> property. For example <code>metadata.customerName === 'John';</code><br/>" +
                "Message type can be accessed via <code>msgType</code> property.<br><br>" +
                "Output connections: <code>True</code>, <code>False</code>, <code>Failure</code>",
        configDirective = "jnksIotFilterNodeScriptConfig"
)
public class JnksIotJsFilterNode implements JnksIotNode {

    private JnksIotJsFilterNodeConfiguration config;
    private ScriptEngine scriptEngine;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotJsFilterNodeConfiguration.class);
        scriptEngine = ctx.createScriptEngine(config.getScriptLang(),
                ScriptLanguage.TBEL.equals(config.getScriptLang()) ? config.getTbelScript() : config.getJsScript());
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        withCallback(scriptEngine.executeFilterAsync(msg),
                filterResult -> {
                    ctx.tellNext(msg, filterResult ? JnksIotNodeConnectionType.TRUE : JnksIotNodeConnectionType.FALSE);
                },
                t -> {
                    ctx.tellFailure(msg, t);
                }, ctx.getDbCallbackExecutor());
    }

    @Override
    public void destroy() {
        if (scriptEngine != null) {
            scriptEngine.destroy();
        }
    }
}
