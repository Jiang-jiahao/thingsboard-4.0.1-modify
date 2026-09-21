package com.jnks.iot.rule.engine.action;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;
import com.jnks.iot.common.util.JacksonUtil;
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

import java.util.Objects;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "log",
        configClazz = JnksIotLogNodeConfiguration.class,
        nodeDescription = "Log incoming messages using JS script for transformation Message into String",
        nodeDetails = "Transform incoming Message with configured JS function to String and log final value into JnksIOT log file. " +
                "Message payload can be accessed via <code>msg</code> property. For example <code>'temperature = ' + msg.temperature ;</code>. " +
                "Message metadata can be accessed via <code>metadata</code> property. For example <code>'name = ' + metadata.customerName;</code>.",
        configDirective = "jnksIotActionNodeLogConfig",
        icon = "menu"
)
public class JnksIotLogNode implements JnksIotNode {

    private JnksIotLogNodeConfiguration config;
    private ScriptEngine scriptEngine;
    private boolean standard;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotLogNodeConfiguration.class);
        this.standard = isStandard(config);
        this.scriptEngine = this.standard ? null : createScriptEngine(ctx, config);
    }

    ScriptEngine createScriptEngine(JnksIotContext ctx, JnksIotLogNodeConfiguration config) {
        return ctx.createScriptEngine(config.getScriptLang(),
                ScriptLanguage.TBEL.equals(config.getScriptLang()) ? config.getTbelScript() : config.getJsScript());
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        if (!log.isInfoEnabled()) {
            ctx.tellSuccess(msg);
            return;
        }
        if (standard) {
            logStandard(ctx, msg);
            return;
        }

        Futures.addCallback(scriptEngine.executeToStringAsync(msg), new FutureCallback<String>() {
            @Override
            public void onSuccess(@Nullable String result) {
                log.info(result);
                ctx.tellSuccess(msg);
            }

            @Override
            public void onFailure(Throwable t) {
                ctx.tellFailure(msg, t);
            }
        }, MoreExecutors.directExecutor()); //usually js responses runs on js callback executor
    }

    boolean isStandard(JnksIotLogNodeConfiguration conf) {
        Objects.requireNonNull(conf, "node config is null");
        final JnksIotLogNodeConfiguration defaultConfig = new JnksIotLogNodeConfiguration().defaultConfiguration();

        if (conf.getScriptLang() == null || conf.getScriptLang().equals(ScriptLanguage.JS)) {
            return defaultConfig.getJsScript().equals(conf.getJsScript());
        } else if (conf.getScriptLang().equals(ScriptLanguage.TBEL)) {
            return defaultConfig.getTbelScript().equals(conf.getTbelScript());
        } else {
            log.warn("No rule to define isStandard script for script language [{}], assuming that is non-standard", conf.getScriptLang());
            return false;
        }
    }

    void logStandard(JnksIotContext ctx, JnksIotMsg msg) {
        log.info(toLogMessage(msg));
        ctx.tellSuccess(msg);
    }

    String toLogMessage(JnksIotMsg msg) {
        return "\n" +
                "Incoming message:\n" + msg.getData() + "\n" +
                "Incoming metadata:\n" + JacksonUtil.toString(msg.getMetaData().getData());
    }

    @Override
    public void destroy() {
        if (scriptEngine != null) {
            scriptEngine.destroy();
        }
    }
}
