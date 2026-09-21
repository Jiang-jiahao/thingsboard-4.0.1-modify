package com.jnks.iot.server.actors.ruleChain;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.msg.JnksIotActorStopReason;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotRuleEngineActorMsg;
import com.jnks.iot.server.common.msg.aware.RuleChainAwareMsg;
import com.jnks.iot.server.common.msg.queue.RuleEngineException;

@EqualsAndHashCode(callSuper = true)
@ToString
public abstract class JnksIotToRuleChainActorMsg extends JnksIotRuleEngineActorMsg implements RuleChainAwareMsg {

    @Getter
    private final RuleChainId target;

    public JnksIotToRuleChainActorMsg(JnksIotMsg msg, RuleChainId target) {
        super(msg);
        this.target = target;
    }

    @Override
    public RuleChainId getRuleChainId() {
        return target;
    }

    @Override
    public void onJnksIotActorStopped(JnksIotActorStopReason reason) {
        String message = reason == JnksIotActorStopReason.STOPPED ? String.format("Rule chain [%s] stopped", target.getId()) : String.format("Failed to initialize rule chain [%s]!", target.getId());
        msg.getCallback().onFailure(new RuleEngineException(message));
    }
}
