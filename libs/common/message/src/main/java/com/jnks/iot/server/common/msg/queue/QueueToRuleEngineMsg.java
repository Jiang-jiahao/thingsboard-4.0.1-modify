package com.jnks.iot.server.common.msg.queue;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.JnksIotActorStopReason;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotRuleEngineActorMsg;

import java.util.Set;

/**
 * Created by ashvayka on 15.03.18.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class QueueToRuleEngineMsg extends JnksIotRuleEngineActorMsg {

    @Getter
    private final TenantId tenantId;
    @Getter
    private final Set<String> relationTypes;
    @Getter
    private final String failureMessage;

    public QueueToRuleEngineMsg(TenantId tenantId, JnksIotMsg jnksIotMsg, Set<String> relationTypes, String failureMessage) {
        super(jnksIotMsg);
        this.tenantId = tenantId;
        this.relationTypes = relationTypes;
        this.failureMessage = failureMessage;
    }

    @Override
    public MsgType getMsgType() {
        return MsgType.QUEUE_TO_RULE_ENGINE_MSG;
    }

    @Override
    public void onJnksIotActorStopped(JnksIotActorStopReason reason) {
        String message;
        if (msg.getRuleChainId() != null) {
            message = reason == JnksIotActorStopReason.STOPPED ?
                    String.format("Rule chain [%s] stopped", msg.getRuleChainId().getId()) :
                    String.format("Failed to initialize rule chain [%s]!", msg.getRuleChainId().getId());
        } else {
            message = reason == JnksIotActorStopReason.STOPPED ? "Rule chain stopped" : "Failed to initialize rule chain!";
        }
        msg.getCallback().onFailure(new RuleEngineException(message));
    }

    public boolean isTellNext() {
        return relationTypes != null && !relationTypes.isEmpty();
    }

}
