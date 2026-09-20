package com.jnks.iot.server.actors.ruleChain;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import com.jnks.iot.rule.engine.api.TbContext;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.TbMsg;

/**
 * Created by ashvayka on 19.03.18.
 */
@EqualsAndHashCode(callSuper = true)
@ToString
final class RuleNodeToSelfMsg extends TbToRuleNodeActorMsg {

    public RuleNodeToSelfMsg(TbContext ctx, TbMsg tbMsg) {
        super(ctx, tbMsg);
    }

    @Override
    public MsgType getMsgType() {
        return MsgType.RULE_TO_SELF_MSG;
    }

}
