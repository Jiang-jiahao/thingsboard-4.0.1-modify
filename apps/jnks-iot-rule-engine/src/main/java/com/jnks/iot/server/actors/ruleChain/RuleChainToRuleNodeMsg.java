package com.jnks.iot.server.actors.ruleChain;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

/**
 * Created by ashvayka on 19.03.18.
 */
@EqualsAndHashCode(callSuper = true)
@ToString
final class RuleChainToRuleNodeMsg extends JnksIotToRuleNodeActorMsg {

    @Getter
    private final String fromRelationType;

    public RuleChainToRuleNodeMsg(JnksIotContext ctx, JnksIotMsg jnksIotMsg, String fromRelationType) {
        super(ctx, jnksIotMsg);
        this.fromRelationType = fromRelationType;
    }

    @Override
    public MsgType getMsgType() {
        return MsgType.RULE_CHAIN_TO_RULE_MSG;
    }
}
