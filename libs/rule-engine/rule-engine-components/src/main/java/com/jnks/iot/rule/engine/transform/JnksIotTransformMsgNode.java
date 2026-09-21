package com.jnks.iot.rule.engine.transform;

import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.ScriptEngine;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.script.ScriptLanguage;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.List;

@RuleNode(
        type = ComponentType.TRANSFORMATION,
        name = "script",
        configClazz = JnksIotTransformMsgNodeConfiguration.class,
        nodeDescription = "Change Message payload, Metadata or Message type using JavaScript",
        nodeDetails = "JavaScript function receive 3 input parameters <br/> " +
                "<code>msg</code> - is a message payload.<br/>" +
                "<code>metadata</code> - is a message metadata.<br/>" +
                "<code>msgType</code> - is a message type.<br/>" +
                "Should return the following structure:<br/>" +
                "<code>{ msg: <i style=\"color: #666;\">new payload</i>,<br/>&nbsp&nbsp&nbspmetadata: <i style=\"color: #666;\">new metadata</i>,<br/>&nbsp&nbsp&nbspmsgType: <i style=\"color: #666;\">new msgType</i> }</code><br/>" +
                "All fields in resulting object are optional and will be taken from original message if not specified.<br><br>" +
                "Output connections: <code>Success</code>, <code>Failure</code>.",
        configDirective = "jnksIotTransformationNodeScriptConfig"
)
public class JnksIotTransformMsgNode extends JnksIotAbstractTransformNode<JnksIotTransformMsgNodeConfiguration> {

    private ScriptEngine scriptEngine;

    @Override
    protected JnksIotTransformMsgNodeConfiguration loadNodeConfiguration(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        var config = JnksIotNodeUtils.convert(configuration, JnksIotTransformMsgNodeConfiguration.class);
        scriptEngine = ctx.createScriptEngine(config.getScriptLang(),
                ScriptLanguage.TBEL.equals(config.getScriptLang()) ? config.getTbelScript() : config.getJsScript());
        return config;
    }

    @Override
    protected ListenableFuture<List<JnksIotMsg>> transform(JnksIotContext ctx, JnksIotMsg msg) {
        return scriptEngine.executeUpdateAsync(msg);
    }

    @Override
    protected void transformFailure(JnksIotContext ctx, JnksIotMsg msg, Throwable t) {
        super.transformFailure(ctx, msg, t);
    }

    @Override
    public void destroy() {
        if (scriptEngine != null) {
            scriptEngine.destroy();
        }
    }
}
