package com.jnks.iot.rule.engine.transform;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.queue.RuleEngineException;
import com.jnks.iot.server.common.msg.queue.JnksIotMsgCallback;

import java.util.List;

import static com.jnks.iot.common.util.DonAsynchron.withCallback;

/**
 * Created by ashvayka on 19.01.18.
 */
@Slf4j
public abstract class JnksIotAbstractTransformNode<C> implements JnksIotNode {

    protected C config;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        config = loadNodeConfiguration(ctx, configuration);
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        withCallback(transform(ctx, msg),
                m -> transformSuccess(ctx, msg, m),
                t -> transformFailure(ctx, msg, t),
                MoreExecutors.directExecutor());
    }

    protected abstract C loadNodeConfiguration(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException;

    protected void transformFailure(JnksIotContext ctx, JnksIotMsg msg, Throwable t) {
        ctx.tellFailure(msg, t);
    }

    protected void transformSuccess(JnksIotContext ctx, JnksIotMsg msg, List<JnksIotMsg> msgs) {
        if (msgs == null || msgs.isEmpty()) {
            ctx.tellFailure(msg, new RuntimeException("Message or messages list are empty!"));
        } else if (msgs.size() == 1) {
            ctx.tellSuccess(msgs.get(0));
        } else {
            JnksIotMsgCallbackWrapper wrapper = new MultipleJnksIotMsgsCallbackWrapper(msgs.size(), new JnksIotMsgCallback() {
                @Override
                public void onSuccess() {
                    ctx.ack(msg);
                }

                @Override
                public void onFailure(RuleEngineException e) {
                    ctx.tellFailure(msg, e);
                }
            });
            msgs.forEach(newMsg -> ctx.enqueueForTellNext(newMsg, JnksIotNodeConnectionType.SUCCESS, wrapper::onSuccess, wrapper::onFailure));
        }
    }

    protected abstract ListenableFuture<List<JnksIotMsg>> transform(JnksIotContext ctx, JnksIotMsg msg);

}
