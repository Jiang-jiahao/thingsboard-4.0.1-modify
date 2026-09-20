package com.jnks.iot.server.actors.ruleChain;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.msg.TbActorStopReason;
import com.jnks.iot.server.common.msg.TbMsg;
import com.jnks.iot.server.common.msg.TbRuleEngineActorMsg;
import com.jnks.iot.server.common.msg.aware.RuleChainAwareMsg;
import com.jnks.iot.server.common.msg.queue.RuleEngineException;

@EqualsAndHashCode(callSuper = true)
@ToString
public abstract class TbToRuleChainActorMsg extends TbRuleEngineActorMsg implements RuleChainAwareMsg {

    @Getter
    private final RuleChainId target;

    public TbToRuleChainActorMsg(TbMsg msg, RuleChainId target) {
        super(msg);
        this.target = target;
    }

    @Override
    public RuleChainId getRuleChainId() {
        return target;
    }

    @Override
    public void onTbActorStopped(TbActorStopReason reason) {
        String message = reason == TbActorStopReason.STOPPED ? String.format("Rule chain [%s] stopped", target.getId()) : String.format("Failed to initialize rule chain [%s]!", target.getId());
        msg.getCallback().onFailure(new RuleEngineException(message));
    }
}
