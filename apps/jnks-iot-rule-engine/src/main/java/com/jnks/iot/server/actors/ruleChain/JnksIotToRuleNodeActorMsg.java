package com.jnks.iot.server.actors.ruleChain;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.server.common.msg.JnksIotActorStopReason;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotRuleEngineActorMsg;
import com.jnks.iot.server.common.msg.queue.RuleNodeException;

@EqualsAndHashCode(callSuper = true)
public abstract class JnksIotToRuleNodeActorMsg extends JnksIotRuleEngineActorMsg {

    @Getter
    private final JnksIotContext ctx;

    public JnksIotToRuleNodeActorMsg(JnksIotContext ctx, JnksIotMsg jnksIotMsg) {
        super(jnksIotMsg);
        this.ctx = ctx;
    }

    @Override
    public void onJnksIotActorStopped(JnksIotActorStopReason reason) {
        String message = reason == JnksIotActorStopReason.STOPPED ? "Rule node stopped" : "Failed to initialize rule node!";
        msg.getCallback().onFailure(new RuleNodeException(message, ctx.getRuleChainName(), ctx.getSelf()));
    }
}
