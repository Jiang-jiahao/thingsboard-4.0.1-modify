package com.jnks.iot.server.actors.ruleChain;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import com.jnks.iot.rule.engine.api.TbContext;
import com.jnks.iot.server.common.msg.TbActorStopReason;
import com.jnks.iot.server.common.msg.TbMsg;
import com.jnks.iot.server.common.msg.TbRuleEngineActorMsg;
import com.jnks.iot.server.common.msg.queue.RuleNodeException;

@EqualsAndHashCode(callSuper = true)
public abstract class TbToRuleNodeActorMsg extends TbRuleEngineActorMsg {

    @Getter
    private final TbContext ctx;

    public TbToRuleNodeActorMsg(TbContext ctx, TbMsg tbMsg) {
        super(tbMsg);
        this.ctx = ctx;
    }

    @Override
    public void onTbActorStopped(TbActorStopReason reason) {
        String message = reason == TbActorStopReason.STOPPED ? "Rule node stopped" : "Failed to initialize rule node!";
        msg.getCallback().onFailure(new RuleNodeException(message, ctx.getRuleChainName(), ctx.getSelf()));
    }
}
