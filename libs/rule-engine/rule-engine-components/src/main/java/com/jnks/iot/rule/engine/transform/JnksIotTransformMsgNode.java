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
        name = "脚本",
        configClazz = JnksIotTransformMsgNodeConfiguration.class,
        nodeDescription = "使用 JavaScript 更改消息负载、元数据或消息类型",
        nodeDetails = "JavaScript 函数接收 3 个输入参数 <br/> " +
                "<code>msg</code> - 消息负载。<br/>" +
                "<code>metadata</code> - 消息元数据。<br/>" +
                "<code>msgType</code> - 消息类型。<br/>" +
                "应返回以下结构：<br/>" +
                "<code>{ msg: <i style=\"color: #666;\">new payload</i>,<br/>&nbsp&nbsp&nbspmetadata: <i style=\"color: #666;\">new metadata</i>,<br/>&nbsp&nbsp&nbspmsgType: <i style=\"color: #666;\">new msgType</i> }</code><br/>" +
                "结果对象中的所有字段都是可选的，如果未指定，将从原始消息中获取。<br><br>" +
                "输出连接：<code>Success</code>、<code>Failure</code>。",
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
