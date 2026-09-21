package com.jnks.iot.rule.engine.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.EmptyNodeConfiguration;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.queue.RuleEngineException;
import com.jnks.iot.server.common.msg.queue.JnksIotMsgCallback;

import java.util.concurrent.ExecutionException;

@Slf4j
@RuleNode(
        type = ComponentType.TRANSFORMATION,
        name = "split array msg",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "Split array message into several messages",
        nodeDetails = "Splits an array message into individual elements, with each element sent as a separate message. " +
                "All outbound messages will have the same type and metadata as the original array message.<br><br>" +
                "Output connections: <code>Success</code>, <code>Failure</code>.",
        icon = "content_copy",
        configDirective = "jnksIotNodeEmptyConfig"
)
public class JnksIotSplitArrayMsgNode implements JnksIotNode {

    private EmptyNodeConfiguration config;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) throws ExecutionException, InterruptedException, JnksIotNodeException {
        JsonNode jsonNode = JacksonUtil.toJsonNode(msg.getData());
        if (jsonNode.isArray()) {
            ArrayNode data = (ArrayNode) jsonNode;
            if (data.isEmpty()) {
                ctx.ack(msg);
            } else if (data.size() == 1) {
                ctx.tellSuccess(msg.transform()
                        .data(JacksonUtil.toString(data.get(0)))
                        .build());
            } else {
                JnksIotMsgCallbackWrapper wrapper = new MultipleJnksIotMsgsCallbackWrapper(data.size(), new JnksIotMsgCallback() {
                    @Override
                    public void onSuccess() {
                        ctx.ack(msg);
                    }

                    @Override
                    public void onFailure(RuleEngineException e) {
                        ctx.tellFailure(msg, e);
                    }
                });
                data.forEach(msgNode -> {
                    JnksIotMsg outMsg = msg.transform()
                            .data(JacksonUtil.toString(msgNode))
                            .build();
                    ctx.enqueueForTellNext(outMsg, JnksIotNodeConnectionType.SUCCESS, wrapper::onSuccess, wrapper::onFailure);
                });
            }
        } else {
            ctx.tellFailure(msg, new RuntimeException("Msg data is not a JSON Array!"));
        }
    }
}
