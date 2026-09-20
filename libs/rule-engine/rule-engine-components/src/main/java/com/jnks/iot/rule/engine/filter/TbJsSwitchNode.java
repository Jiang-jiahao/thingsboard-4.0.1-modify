package com.jnks.iot.rule.engine.filter;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.ScriptEngine;
import com.jnks.iot.rule.engine.api.TbContext;
import com.jnks.iot.rule.engine.api.TbNode;
import com.jnks.iot.rule.engine.api.TbNodeConfiguration;
import com.jnks.iot.rule.engine.api.TbNodeException;
import com.jnks.iot.rule.engine.api.util.TbNodeUtils;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.script.ScriptLanguage;
import com.jnks.iot.server.common.msg.TbMsg;

import java.util.Set;

@Slf4j
@RuleNode(
        type = ComponentType.FILTER,
        name = "switch", customRelations = true,
        relationTypes = {},
        configClazz = TbJsSwitchNodeConfiguration.class,
        nodeDescription = "Routes incoming message to one OR multiple output connections.",
        nodeDetails = "Node executes configured TBEL(recommended) or JavaScript function that returns array of strings (connection names). " +
                "If Array is empty - message not routed to next Node. " +
                "Message payload can be accessed via <code>msg</code> property. For example <code>msg.temperature < 10;</code><br/>" +
                "Message metadata can be accessed via <code>metadata</code> property. For example <code>metadata.customerName === 'John';</code><br/>" +
                "Message type can be accessed via <code>msgType</code> property.<br><br>" +
                "Output connections: <i>Custom connection(s) defined by switch node</i> or <code>Failure</code>",
        configDirective = "tbFilterNodeSwitchConfig")
public class TbJsSwitchNode implements TbNode {

    private TbJsSwitchNodeConfiguration config;
    private ScriptEngine scriptEngine;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbJsSwitchNodeConfiguration.class);
        this.scriptEngine = ctx.createScriptEngine(config.getScriptLang(),
                ScriptLanguage.TBEL.equals(config.getScriptLang()) ? config.getTbelScript() : config.getJsScript());
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) {
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

    private void processSwitch(TbContext ctx, TbMsg msg, Set<String> nextRelations) {
        ctx.tellNext(msg, nextRelations);
    }

    @Override
    public void destroy() {
        if (scriptEngine != null) {
            scriptEngine.destroy();
        }
    }
}
