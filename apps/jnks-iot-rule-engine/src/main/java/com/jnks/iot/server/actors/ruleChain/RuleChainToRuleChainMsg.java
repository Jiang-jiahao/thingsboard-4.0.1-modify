package com.jnks.iot.server.actors.ruleChain;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

/**
 * Created by ashvayka on 19.03.18.
 */
@EqualsAndHashCode(callSuper = true)
@ToString
public final class RuleChainToRuleChainMsg extends JnksIotToRuleChainActorMsg  {

    @Getter
    private final RuleChainId source;
    @Getter
    private final String fromRelationType;

    public RuleChainToRuleChainMsg(RuleChainId target, RuleChainId source, JnksIotMsg jnksIotMsg, String fromRelationType) {
        super(jnksIotMsg, target);
        this.source = source;
        this.fromRelationType = fromRelationType;
    }

    @Override
    public MsgType getMsgType() {
        return MsgType.RULE_CHAIN_TO_RULE_CHAIN_MSG;
    }
}
