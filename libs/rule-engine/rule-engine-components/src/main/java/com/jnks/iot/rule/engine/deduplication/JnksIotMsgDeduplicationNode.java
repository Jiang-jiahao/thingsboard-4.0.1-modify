package com.jnks.iot.rule.engine.deduplication;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.jnks.iot.server.common.data.DataConstants.QUEUE_NAME;

@RuleNode(
        type = ComponentType.TRANSFORMATION,
        name = "deduplication",
        configClazz = JnksIotMsgDeduplicationNodeConfiguration.class,
        version = 1,
        hasQueueName = true,
        nodeDescription = "Deduplicate messages within the same originator entity for a configurable period " +
                "based on a specified deduplication strategy.",
        nodeDetails = "Deduplication strategies: <ul><li><strong>FIRST</strong> - return first message that arrived during deduplication period.</li>" +
                "<li><strong>LAST</strong> - return last message that arrived during deduplication period.</li>" +
                "<li><strong>ALL</strong> - return all messages as a single JSON array message. " +
                "Where each element represents object with <strong><i>msg</i></strong> and <strong><i>metadata</i></strong> inner properties.</li></ul>",
        icon = "content_copy",
        configDirective = "jnksIotTransformationNodeDeduplicationConfig"
)
@Slf4j
public class JnksIotMsgDeduplicationNode implements JnksIotNode {

    public static final int JNKS_IOT_MSG_DEDUPLICATION_RETRY_DELAY = 10;

    private JnksIotMsgDeduplicationNodeConfiguration config;

    private final Map<EntityId, DeduplicationData> deduplicationMap;
    private long deduplicationInterval;
    private String queueName;

    public JnksIotMsgDeduplicationNode() {
        this.deduplicationMap = new HashMap<>();
    }

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotMsgDeduplicationNodeConfiguration.class);
        this.deduplicationInterval = TimeUnit.SECONDS.toMillis(config.getInterval());
        this.queueName = ctx.getQueueName();
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) throws ExecutionException, InterruptedException, JnksIotNodeException {
        if (msg.isTypeOf(JnksIotMsgType.DEDUPLICATION_TIMEOUT_SELF_MSG)) {
            processDeduplication(ctx, msg.getOriginator());
        } else {
            processOnRegularMsg(ctx, msg);
        }
    }

    @Override
    public void destroy() {
        deduplicationMap.clear();
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        boolean hasChanges = false;
        switch (fromVersion) {
            case 0:
                if (oldConfiguration.has(QUEUE_NAME)) {
                    hasChanges = true;
                    ((ObjectNode) oldConfiguration).remove(QUEUE_NAME);
                }
                break;
            default:
                break;
        }
        return new JnksIotPair<>(hasChanges, oldConfiguration);
    }

    private void processOnRegularMsg(JnksIotContext ctx, JnksIotMsg msg) {
        EntityId id = msg.getOriginator();
        DeduplicationData deduplicationMsgs = deduplicationMap.computeIfAbsent(id, k -> new DeduplicationData());
        if (deduplicationMsgs.size() < config.getMaxPendingMsgs()) {
            log.trace("[{}][{}] Adding msg: [{}][{}] to the pending msgs map ...", ctx.getSelfId(), id, msg.getId(), msg.getMetaDataTs());
            deduplicationMsgs.add(msg);
            ctx.ack(msg);
            scheduleTickMsg(ctx, id, deduplicationMsgs);
        } else {
            log.trace("[{}] Max limit of pending messages reached for deduplication id: [{}]", ctx.getSelfId(), id);
            ctx.tellFailure(msg, new RuntimeException("[" + ctx.getSelfId() + "] Max limit of pending messages reached for deduplication id: [" + id + "]"));
        }
    }

    private void processDeduplication(JnksIotContext ctx, EntityId deduplicationId) {
        DeduplicationData data = deduplicationMap.get(deduplicationId);
        if (data == null) {
            return;
        }
        data.setTickScheduled(false);
        if (data.isEmpty()) {
            return;
        }
        long deduplicationTimeoutMs = System.currentTimeMillis();
        try {
            List<JnksIotMsg> deduplicationResults = new ArrayList<>();
            List<JnksIotMsg> msgList = data.getMsgList();
            Optional<JnksIotPair<Long, Long>> packBoundsOpt = findValidPack(msgList, deduplicationTimeoutMs);
            while (packBoundsOpt.isPresent()) {
                JnksIotPair<Long, Long> packBounds = packBoundsOpt.get();
                if (DeduplicationStrategy.ALL.equals(config.getStrategy())) {
                    List<JnksIotMsg> pack = new ArrayList<>();
                    for (Iterator<JnksIotMsg> iterator = msgList.iterator(); iterator.hasNext(); ) {
                        JnksIotMsg msg = iterator.next();
                        long msgTs = msg.getMetaDataTs();
                        if (msgTs >= packBounds.getFirst() && msgTs < packBounds.getSecond()) {
                            pack.add(msg);
                            iterator.remove();
                        }
                    }
                    deduplicationResults.add(JnksIotMsg.newMsg()
                            .queueName(queueName)
                            .type(config.getOutMsgType())
                            .originator(deduplicationId)
                            .copyMetaData(getMetadata())
                            .data(getMergedData(pack))
                            .build());
                } else {
                    JnksIotMsg resultMsg = null;
                    boolean searchMin = DeduplicationStrategy.FIRST.equals(config.getStrategy());
                    for (Iterator<JnksIotMsg> iterator = msgList.iterator(); iterator.hasNext(); ) {
                        JnksIotMsg msg = iterator.next();
                        long msgTs = msg.getMetaDataTs();
                        if (msgTs >= packBounds.getFirst() && msgTs < packBounds.getSecond()) {
                            iterator.remove();
                            if (resultMsg == null
                                    || (searchMin && msg.getMetaDataTs() < resultMsg.getMetaDataTs())
                                    || (!searchMin && msg.getMetaDataTs() > resultMsg.getMetaDataTs())) {
                                resultMsg = msg;
                            }
                        }
                    }
                    if (resultMsg != null) {
                        String queueName1 = queueName != null ? queueName : resultMsg.getQueueName();
                        deduplicationResults.add(JnksIotMsg.newMsg()
                                .queueName(queueName1)
                                .type(resultMsg.getType())
                                .originator(resultMsg.getOriginator())
                                .customerId(resultMsg.getCustomerId())
                                .copyMetaData(resultMsg.getMetaData())
                                .data(resultMsg.getData())
                                .build());
                    }
                }
                packBoundsOpt = findValidPack(msgList, deduplicationTimeoutMs);
            }
            deduplicationResults.forEach(outMsg -> enqueueForTellNextWithRetry(ctx, outMsg, 0));
        } finally {
            if (!data.isEmpty()) {
                scheduleTickMsg(ctx, deduplicationId, data);
            }
        }
    }

    private void scheduleTickMsg(JnksIotContext ctx, EntityId deduplicationId, DeduplicationData data) {
        if (!data.isTickScheduled()) {
            scheduleTickMsg(ctx, deduplicationId);
            data.setTickScheduled(true);
        }
    }

    private Optional<JnksIotPair<Long, Long>> findValidPack(List<JnksIotMsg> msgs, long deduplicationTimeoutMs) {
        Optional<JnksIotMsg> min = msgs.stream().min(Comparator.comparing(JnksIotMsg::getMetaDataTs));
        return min.map(minTsMsg -> {
            long packStartTs = minTsMsg.getMetaDataTs();
            long packEndTs = packStartTs + deduplicationInterval;
            if (packEndTs <= deduplicationTimeoutMs) {
                return new JnksIotPair<>(packStartTs, packEndTs);
            }
            return null;
        });
    }

    private void enqueueForTellNextWithRetry(JnksIotContext ctx, JnksIotMsg msg, int retryAttempt) {
        if (config.getMaxRetries() > retryAttempt) {
            ctx.enqueueForTellNext(msg, JnksIotNodeConnectionType.SUCCESS,
                    () -> {
                        log.trace("[{}][{}][{}] Successfully enqueue deduplication result message!", ctx.getSelfId(), msg.getOriginator(), retryAttempt);
                    },
                    throwable -> {
                        log.trace("[{}][{}][{}] Failed to enqueue deduplication output message due to: ", ctx.getSelfId(), msg.getOriginator(), retryAttempt, throwable);
                        ctx.schedule(() -> {
                            enqueueForTellNextWithRetry(ctx, msg, retryAttempt + 1);
                        }, JNKS_IOT_MSG_DEDUPLICATION_RETRY_DELAY, TimeUnit.SECONDS);
                    });
        }
    }

    private void scheduleTickMsg(JnksIotContext ctx, EntityId deduplicationId) {
        ctx.tellSelf(ctx.newMsg(null, JnksIotMsgType.DEDUPLICATION_TIMEOUT_SELF_MSG, deduplicationId, JnksIotMsgMetaData.EMPTY, JnksIotMsg.EMPTY_STRING), deduplicationInterval + 1);
    }

    private String getMergedData(List<JnksIotMsg> msgs) {
        ArrayNode mergedData = JacksonUtil.newArrayNode();
        msgs.forEach(msg -> {
            ObjectNode msgNode = JacksonUtil.newObjectNode();
            msgNode.set("msg", JacksonUtil.toJsonNode(msg.getData()));
            msgNode.set("metadata", JacksonUtil.valueToTree(msg.getMetaData().getData()));
            mergedData.add(msgNode);
        });
        return JacksonUtil.toString(mergedData);
    }

    private JnksIotMsgMetaData getMetadata() {
        JnksIotMsgMetaData metaData = new JnksIotMsgMetaData();
        metaData.putValue("ts", String.valueOf(System.currentTimeMillis()));
        return metaData;
    }

}
