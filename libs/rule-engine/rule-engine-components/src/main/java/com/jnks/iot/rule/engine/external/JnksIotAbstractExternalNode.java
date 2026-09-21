package com.jnks.iot.rule.engine.external;

import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

public abstract class JnksIotAbstractExternalNode implements JnksIotNode {

    private boolean forceAck;

    public void init(JnksIotContext ctx) {
        this.forceAck = ctx.isExternalNodeForceAck();
    }

    protected void tellSuccess(JnksIotContext ctx, JnksIotMsg jnksIotMsg) {
        if (forceAck) {
            ctx.enqueueForTellNext(jnksIotMsg.copyWithNewCtx(), JnksIotNodeConnectionType.SUCCESS);
        } else {
            ctx.tellSuccess(jnksIotMsg);
        }
    }

    protected void tellFailure(JnksIotContext ctx, JnksIotMsg jnksIotMsg, Throwable t) {
        if (forceAck) {
            if (t == null) {
                ctx.enqueueForTellNext(jnksIotMsg.copyWithNewCtx(), JnksIotNodeConnectionType.FAILURE);
            } else {
                ctx.enqueueForTellFailure(jnksIotMsg.copyWithNewCtx(), t);
            }
        } else {
            if (t == null) {
                ctx.tellNext(jnksIotMsg, JnksIotNodeConnectionType.FAILURE);
            } else {
                ctx.tellFailure(jnksIotMsg, t);
            }
        }
    }

    protected JnksIotMsg ackIfNeeded(JnksIotContext ctx, JnksIotMsg msg) {
        if (forceAck) {
            ctx.ack(msg);
            return msg.copyWithNewCtx();
        } else {
            return msg;
        }
    }

}
