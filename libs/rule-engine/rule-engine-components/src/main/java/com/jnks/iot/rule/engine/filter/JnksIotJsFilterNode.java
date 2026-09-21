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
        name = "脚本过滤",
        relationTypes = {JnksIotNodeConnectionType.TRUE, JnksIotNodeConnectionType.FALSE},
        configClazz = JnksIotJsFilterNodeConfiguration.class,
        nodeDescription = "使用 TBEL 或 JS 脚本过滤传入消息",
        nodeDetails = "使用传入消息求值布尔函数。 " +
                "该函数可用 TBEL 或纯 JavaScript 编写。 " +
                "脚本函数应返回布尔值，并接受三个参数：<br/>" +
                "可通过 <code>msg</code> 属性访问消息负载。例如 <code>msg.temperature < 10;</code><br/>" +
                "可通过 <code>metadata</code> 属性访问消息元数据。例如 <code>metadata.customerName === 'John';</code><br/>" +
                "可通过 <code>msgType</code> 属性访问消息类型。<br><br>" +
                "输出连接：<code>True</code>、<code>False</code>、<code>Failure</code>",
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
