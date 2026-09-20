package com.jnks.iot.server.actors.ruleChain;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.TbMsg;

/**
 * Created by ashvayka on 19.03.18.
 */
@EqualsAndHashCode(callSuper = true)
@ToString
public final class RuleChainInputMsg extends TbToRuleChainActorMsg {

    public RuleChainInputMsg(RuleChainId target, TbMsg tbMsg) {
        super(tbMsg, target);
    }

    @Override
    public MsgType getMsgType() {
        return MsgType.RULE_CHAIN_INPUT_MSG;
    }
}
