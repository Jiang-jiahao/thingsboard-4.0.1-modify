package com.jnks.iot.server.actors.ruleChain;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.id.RuleNodeId;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.JnksIotActorStopReason;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotRuleEngineActorMsg;
import com.jnks.iot.server.common.msg.queue.RuleEngineException;

import java.io.Serial;
import java.io.Serializable;
import java.util.Set;

/**
 * Created by ashvayka on 19.03.18.
 */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString
public class RuleNodeToRuleChainTellNextMsg extends JnksIotRuleEngineActorMsg implements Serializable {

    @Serial
    private static final long serialVersionUID = 4577026446412871820L;

    private final RuleChainId ruleChainId;
    private final RuleNodeId originator;
    private final Set<String> relationTypes;
    private final String failureMessage;

    public RuleNodeToRuleChainTellNextMsg(RuleChainId ruleChainId, RuleNodeId originator, Set<String> relationTypes, JnksIotMsg jnksIotMsg, String failureMessage) {
        super(jnksIotMsg);
        this.ruleChainId = ruleChainId;
        this.originator = originator;
        this.relationTypes = relationTypes;
        this.failureMessage = failureMessage;
    }

    @Override
    public void onJnksIotActorStopped(JnksIotActorStopReason reason) {
        String message = reason == JnksIotActorStopReason.STOPPED ? String.format("Rule chain [%s] stopped", ruleChainId.getId()) : String.format("Failed to initialize rule chain [%s]!", ruleChainId.getId());
        msg.getCallback().onFailure(new RuleEngineException(message));
    }

    @Override
    public MsgType getMsgType() {
        return MsgType.RULE_TO_RULE_CHAIN_TELL_NEXT_MSG;
    }

}
