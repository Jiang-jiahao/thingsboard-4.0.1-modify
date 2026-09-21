package com.jnks.iot.rule.engine.filter;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.ScriptEngine;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.script.ScriptLanguage;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.Set;

@Slf4j
@RuleNode(
        type = ComponentType.FILTER,
        name = "分支", customRelations = true,
        relationTypes = {},
        configClazz = JnksIotJsSwitchNodeConfiguration.class,
        nodeDescription = "将传入消息路由到一个或多个输出连接。",
        nodeDetails = "节点执行已配置的 TBEL（推荐）或 JavaScript 函数，该函数返回字符串数组（连接名称）。 " +
                "如果数组为空，则消息不会路由到下一个节点。 " +
                "可以通过 <code>msg</code> 属性访问消息负载。例如 <code>msg.temperature < 10;</code><br/>" +
                "可以通过 <code>metadata</code> 属性访问消息元数据。例如 <code>metadata.customerName === 'John';</code><br/>" +
                "可以通过 <code>msgType</code> 属性访问消息类型。<br><br>" +
                "输出连接：<i>由分支节点定义的自定义连接</i>或 <code>Failure</code>",
        configDirective = "jnksIotFilterNodeSwitchConfig")
public class JnksIotJsSwitchNode implements JnksIotNode {

    private JnksIotJsSwitchNodeConfiguration config;
    private ScriptEngine scriptEngine;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotJsSwitchNodeConfiguration.class);
        this.scriptEngine = ctx.createScriptEngine(config.getScriptLang(),
                ScriptLanguage.TBEL.equals(config.getScriptLang()) ? config.getTbelScript() : config.getJsScript());
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        Futures.addCallback(scriptEngine.executeSwitchAsync(msg), new FutureCallback<>() {
            @Override
            public void onSuccess(@Nullable Set<String> result) {
                processSwitch(ctx, msg, result);
            }

            @Override
            public void onFailure(Throwable t) {
                ctx.tellFailure(msg, t);
            }
        }, MoreExecutors.directExecutor()); //usually runs in a callbackExecutor
    }

    private void processSwitch(JnksIotContext ctx, JnksIotMsg msg, Set<String> nextRelations) {
        ctx.tellNext(msg, nextRelations);
    }

    @Override
    public void destroy() {
        if (scriptEngine != null) {
            scriptEngine.destroy();
        }
    }
}
