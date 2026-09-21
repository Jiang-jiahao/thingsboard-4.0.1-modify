package com.jnks.iot.server.common.msg;

import lombok.Data;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.id.RuleNodeId;
import com.jnks.iot.server.common.msg.gen.MsgProtos;

import java.io.Serializable;
import java.util.UUID;

@Data
public class JnksIotMsgProcessingStackItem implements Serializable {

    private final RuleChainId ruleChainId;
    private final RuleNodeId ruleNodeId;

    MsgProtos.JnksIotMsgProcessingStackItemProto toProto() {
        return MsgProtos.JnksIotMsgProcessingStackItemProto.newBuilder()
                .setRuleChainIdMSB(ruleChainId.getId().getMostSignificantBits())
                .setRuleChainIdLSB(ruleChainId.getId().getLeastSignificantBits())
                .setRuleNodeIdMSB(ruleNodeId.getId().getMostSignificantBits())
                .setRuleNodeIdLSB(ruleNodeId.getId().getLeastSignificantBits())
                .build();
    }

    static JnksIotMsgProcessingStackItem fromProto(MsgProtos.JnksIotMsgProcessingStackItemProto item){
        return new JnksIotMsgProcessingStackItem(
                new RuleChainId(new UUID(item.getRuleChainIdMSB(), item.getRuleChainIdLSB())),
                new RuleNodeId(new UUID(item.getRuleNodeIdMSB(), item.getRuleNodeIdLSB()))
        );
    }

}
