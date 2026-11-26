package com.jnks.iot.rule.engine.rocketmq;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.external.JnksIotAbstractExternalNode;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.nio.charset.StandardCharsets;

@Slf4j
@RuleNode(
        type = ComponentType.EXTERNAL,
        name = "RocketMQ",
        configClazz = JnksIotRocketMqNodeConfiguration.class,
        version = 1,
        nodeDescription = "将消息发布到 RocketMQ",
        nodeDetails = "将通过 RocketMQ 生产者向 RocketMQ 服务器发送消息。" +
                " 出站消息将包含响应字段（<code>msgId</code>、<code>topic</code>、<code>queueId</code>、<code>queueOffset</code>），" +
                " 这些字段来自 RocketMQ，位于消息元数据中。例如，可通过 <code>metadata.msgId</code> 访问 <b>msgId</b> 字段。",
        configDirective = "jnksIotExternalNodeRocketMqConfig",
        iconUrl = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCIgZmlsbD0iIzAwMCI+PHBhdGggZD0iTTEyIDEuOGMzLjMgMi4zIDUuMiA2IDUuMiA5LjkgMCAxLjYtLjMgMy4yLS45IDQuN2wtNC4zIDMuMi00LjMtMy4yYy0uNi0xLjUtLjktMy4xLS45LTQuNyAwLTMuOSAxLjktNy42IDUuMi05Ljl6bTAgNC4xYTEuOSAxLjkgMCAxIDAgMCAzLjggMS45IDEuOSAwIDAgMCAwLTMuOHoiLz48cGF0aCBkPSJNNy4yIDE2LjYgNSAyMC4zbDMuOS0xLjJ6TTE2LjggMTYuNiAxOSAyMC4zbC0zLjktMS4yeiIvPjxwYXRoIGQ9Ik05LjkgMTkuMiAxMiAyM2wyLjEtMy44eiIvPjwvc3ZnPg=="
)
public class JnksIotRocketMqNode extends JnksIotAbstractExternalNode {

    public enum SendMode {
        ASYNC, SYNC, ONEWAY
    }

    private static final String MSG_ID = "msgId";
    private static final String TOPIC = "topic";
    private static final String QUEUE_ID = "queueId";
    private static final String QUEUE_OFFSET = "queueOffset";
    private static final String STATUS = "status";
    private static final String ERROR = "error";
    static final String JNKS_IOT_MSG_MD_PREFIX = "jnks_iot_msg_md_";

    private JnksIotRocketMqNodeConfiguration config;
    private SendMode sendMode;
    private boolean addMetadataKeyValuesAsProperties;

    private DefaultMQProducer producer;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        super.init(ctx);
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotRocketMqNodeConfiguration.class);
        try {
            this.sendMode = StringUtils.isEmpty(config.getSendMode())
                    ? SendMode.ASYNC
                    : SendMode.valueOf(config.getSendMode().toUpperCase());
            this.addMetadataKeyValuesAsProperties = BooleanUtils.toBooleanDefaultIfNull(config.isAddMetadataKeyValuesAsProperties(), false);
            this.producer = getProducer(ctx);
            this.producer.start();
        } catch (Exception e) {
            throw new JnksIotNodeException(e);
        }
    }

    DefaultMQProducer getProducer(JnksIotContext ctx) {
        RPCHook rpcHook = null;
        if (StringUtils.isNoneBlank(config.getAccessKey(), config.getSecretKey())) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(config.getAccessKey(), config.getSecretKey()));
        }
        DefaultMQProducer result = new DefaultMQProducer(config.getProducerGroup(), rpcHook);
        if (StringUtils.isNotBlank(config.getNamespace())) {
            // setNamespaceV2() writes a separate field, so the classic namespace stays the one applied to topics and groups
            result.setNamespace(config.getNamespace());
        }
        result.setNamesrvAddr(config.getNameServerAddress());
        result.setInstanceName("producer-tb-rocketmq-node-" + ctx.getSelfId().getId().toString() + "-" + ctx.getServiceId());
        result.setSendMsgTimeout(config.getSendMsgTimeout());
        result.setRetryTimesWhenSendFailed(config.getRetryTimes());
        return result;
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        String topic = JnksIotNodeUtils.processPattern(config.getTopicPattern(), msg);
        String tag = isEmptyPattern(config.getTagPattern()) ? null : JnksIotNodeUtils.processPattern(config.getTagPattern(), msg);
        String key = isEmptyPattern(config.getKeyPattern()) ? null : JnksIotNodeUtils.processPattern(config.getKeyPattern(), msg);
        var jnksIotMsg = ackIfNeeded(ctx, msg);
        try {
            ctx.getExternalCallExecutor().executeAsync(() -> {
                publish(ctx, jnksIotMsg, topic, tag, key);
                return null;
            });
        } catch (Exception e) {
            ctx.tellFailure(jnksIotMsg, e);
        }
    }

    private boolean isEmptyPattern(String pattern) {
        return pattern == null || pattern.isEmpty();
    }

    protected void publish(JnksIotContext ctx, JnksIotMsg msg, String topic, String tag, String key) {
        try {
            Message message = new Message(topic, tag, key, msg.getData().getBytes(StandardCharsets.UTF_8));
            if (addMetadataKeyValuesAsProperties) {
                msg.getMetaData().values().forEach((k, v) ->
                        message.putUserProperty(toPropertyName(JNKS_IOT_MSG_MD_PREFIX + k), v));
            }
            switch (sendMode) {
                case SYNC -> tellSuccess(ctx, processResponse(msg, producer.send(message)));
                case ONEWAY -> {
                    producer.sendOneway(message);
                    tellSuccess(ctx, msg);
                }
                default -> producer.send(message, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        tellSuccess(ctx, processResponse(msg, sendResult));
                    }

                    @Override
                    public void onException(Throwable e) {
                        tellFailure(ctx, processException(msg, e), e);
                    }
                });
            }
        } catch (Exception e) {
            log.debug("[{}] Failed to process message: {}", ctx.getSelfId(), msg, e);
            tellFailure(ctx, processException(msg, e), e);
        }
    }

    private static String toPropertyName(String name) {
        return name.replaceAll("[^a-zA-Z0-9_-]", "_");
    }

    @Override
    public void destroy() {
        if (this.producer != null) {
            try {
                this.producer.shutdown();
            } catch (Exception e) {
                log.error("Failed to shutdown producer during destroy()", e);
            }
        }
    }

    private JnksIotMsg processResponse(JnksIotMsg origMsg, SendResult sendResult) {
        JnksIotMsgMetaData metaData = origMsg.getMetaData().copy();
        metaData.putValue(MSG_ID, sendResult.getMsgId());
        metaData.putValue(TOPIC, sendResult.getMessageQueue().getTopic());
        metaData.putValue(QUEUE_ID, String.valueOf(sendResult.getMessageQueue().getQueueId()));
        metaData.putValue(QUEUE_OFFSET, String.valueOf(sendResult.getQueueOffset()));
        metaData.putValue(STATUS, sendResult.getSendStatus().name());
        return origMsg.transform()
                .metaData(metaData)
                .build();
    }

    private JnksIotMsg processException(JnksIotMsg origMsg, Throwable e) {
        JnksIotMsgMetaData metaData = origMsg.getMetaData().copy();
        metaData.putValue(ERROR, e.getClass() + ": " + e.getMessage());
        return origMsg.transform()
                .metaData(metaData)
                .build();
    }
}
