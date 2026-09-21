package com.jnks.iot.rule.engine.filter;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.alarm.Alarm;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.Objects;

@Slf4j
@RuleNode(
        type = ComponentType.FILTER,
        name = "alarm status filter",
        configClazz = JnksIotCheckAlarmStatusNodeConfig.class,
        relationTypes = {JnksIotNodeConnectionType.TRUE, JnksIotNodeConnectionType.FALSE},
        nodeDescription = "Checks alarm status.",
        nodeDetails = "Checks the alarm status to match one of the specified statuses.<br><br>" +
                "Output connections: <code>True</code>, <code>False</code>, <code>Failure</code>.",
        configDirective = "jnksIotFilterNodeCheckAlarmStatusConfig")
public class JnksIotCheckAlarmStatusNode implements JnksIotNode {

    private JnksIotCheckAlarmStatusNodeConfig config;

    @Override
    public void init(JnksIotContext jnksIotContext, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotCheckAlarmStatusNodeConfig.class);
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) throws JnksIotNodeException {
        try {
            Alarm alarm = JacksonUtil.fromString(msg.getData(), Alarm.class);
            Objects.requireNonNull(alarm, "alarm is null");
            ListenableFuture<Alarm> latest = ctx.getAlarmService().findAlarmByIdAsync(ctx.getTenantId(), alarm.getId());

            Futures.addCallback(latest, new FutureCallback<>() {
                @Override
                public void onSuccess(@Nullable Alarm result) {
                    if (result == null) {
                        ctx.tellFailure(msg, new JnksIotNodeException("No such alarm found."));
                        return;
                    }
                    boolean isPresent = config.getAlarmStatusList().stream()
                            .anyMatch(alarmStatus -> result.getStatus() == alarmStatus);
                    ctx.tellNext(msg, isPresent ? JnksIotNodeConnectionType.TRUE : JnksIotNodeConnectionType.FALSE);
                }

                @Override
                public void onFailure(Throwable t) {
                    ctx.tellFailure(msg, t);
                }
            }, ctx.getDbCallbackExecutor());
        } catch (Exception e) {
            if (e instanceof IllegalArgumentException || e instanceof NullPointerException) {
                log.debug("[{}][{}] Failed to parse alarm: [{}] error [{}]", ctx.getTenantId(), ctx.getRuleChainName(), msg.getData(), e.getMessage());
            } else {
                log.error("[{}][{}] Failed to parse alarm: [{}]", ctx.getTenantId(), ctx.getRuleChainName(), msg.getData(), e);
            }
            throw new JnksIotNodeException(e);
        }
    }

}
