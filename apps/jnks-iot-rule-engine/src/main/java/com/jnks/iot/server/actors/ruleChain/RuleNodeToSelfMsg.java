package com.jnks.iot.server.actors.ruleChain;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

/**
 * Created by ashvayka on 19.03.18.
 */
@EqualsAndHashCode(callSuper = true)
@ToString
final class RuleNodeToSelfMsg extends JnksIotToRuleNodeActorMsg {

    public RuleNodeToSelfMsg(JnksIotContext ctx, JnksIotMsg jnksIotMsg) {
        super(ctx, jnksIotMsg);
    }

    @Override
    public MsgType getMsgType() {
        return MsgType.RULE_TO_SELF_MSG;
    }

}
