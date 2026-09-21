package com.jnks.iot.server.service.queue.ruleengine;

import com.google.protobuf.ProtocolStringList;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.EntityIdFactory;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.queue.Queue;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.gen.MsgProtos;
import com.jnks.iot.server.common.msg.queue.QueueToRuleEngineMsg;
import com.jnks.iot.server.common.msg.queue.RuleEngineException;
import com.jnks.iot.server.common.msg.queue.RuleNodeInfo;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.common.msg.queue.JnksIotMsgCallback;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.gen.transport.TransportProtos.ToRuleEngineMsg;
import com.jnks.iot.server.queue.JnksIotQueueConsumer;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.queue.common.consumer.MainQueueConsumerManager;
import com.jnks.iot.server.queue.common.consumer.JnksIotQueueConsumerManagerTask;
import com.jnks.iot.server.queue.common.consumer.JnksIotQueueConsumerManagerTask.DeleteQueueTask;
import com.jnks.iot.server.queue.common.consumer.JnksIotQueueConsumerTask;
import com.jnks.iot.server.queue.discovery.QueueKey;
import com.jnks.iot.server.service.queue.JnksIotMsgPackCallback;
import com.jnks.iot.server.service.queue.JnksIotMsgPackProcessingContext;
import com.jnks.iot.server.service.queue.JnksIotRuleEngineConsumerStats;
import com.jnks.iot.server.service.queue.processing.JnksIotRuleEngineProcessingDecision;
import com.jnks.iot.server.service.queue.processing.JnksIotRuleEngineProcessingResult;
import com.jnks.iot.server.service.queue.processing.JnksIotRuleEngineProcessingStrategy;
import com.jnks.iot.server.service.queue.processing.JnksIotRuleEngineSubmitStrategy;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class JnksIotRuleEngineQueueConsumerManager extends MainQueueConsumerManager<JnksIotProtoQueueMsg<ToRuleEngineMsg>, Queue> {

    public static final String SUCCESSFUL_STATUS = "successful";
    public static final String FAILED_STATUS = "failed";

    private final JnksIotRuleEngineConsumerContext ctx;
    private final JnksIotRuleEngineConsumerStats stats;

    @Builder(builderMethodName = "create") // not to conflict with super.builder()
    public JnksIotRuleEngineQueueConsumerManager(JnksIotRuleEngineConsumerContext ctx,
                                            QueueKey queueKey,
                                            ExecutorService consumerExecutor,
                                            ScheduledExecutorService scheduler,
                                            ExecutorService taskExecutor) {
        super(queueKey, null, null,
                (queueConfig, tpi) -> {
                    Integer partitionId = tpi != null ? tpi.getPartition().orElse(-1) : null;
                    return ctx.getQueueFactory().createToRuleEngineMsgConsumer(queueConfig, partitionId);
                },
                consumerExecutor, scheduler, taskExecutor, null);
        this.ctx = ctx;
        this.stats = new JnksIotRuleEngineConsumerStats(queueKey, ctx.getStatsFactory());
    }

    public void delete(boolean drainQueue) {
        addTask(new DeleteQueueTask(drainQueue));
    }

    @Override
    protected void processTask(JnksIotQueueConsumerManagerTask task) {
        if (task instanceof DeleteQueueTask deleteQueueTask) {
            doDelete(deleteQueueTask.drainQueue());
        }
    }

    private void doDelete(boolean drainQueue) {
        stopped = true;
        log.info("[{}] Handling queue deletion", queueKey);
        consumerWrapper.getConsumers().forEach(JnksIotQueueConsumerTask::awaitCompletion);

        List<JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToRuleEngineMsg>>> queueConsumers = consumerWrapper.getConsumers().stream()
                .map(JnksIotQueueConsumerTask::getConsumer).collect(Collectors.toList());
        consumerExecutor.submit(() -> {
            if (drainQueue) {
                drainQueue(queueConsumers);
            }

            queueConsumers.forEach(consumer -> {
                for (String topic : consumer.getFullTopicNames()) {
                    try {
                        ctx.getQueueAdmin().deleteTopic(topic);
                        log.info("Deleted topic {}", topic);
                    } catch (Exception e) {
                        log.error("Failed to delete topic {}", topic, e);
                    }
                }
                try {
                    consumer.unsubscribe();
                } catch (Exception e) {
                    log.error("[{}] Failed to unsubscribe consumer", queueKey, e);
                }
            });
        });
    }

    /**
     * 消息处理方法
     * @param msgs 消息列表
     * @param consumer 消费者实例
     * @param queue 队列配置
     * @throws Exception
     */
    @Override
    protected void processMsgs(List<JnksIotProtoQueueMsg<ToRuleEngineMsg>> msgs,
                               JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToRuleEngineMsg>> consumer,
                               Queue queue) throws Exception {
        // 获取提交策略
        JnksIotRuleEngineSubmitStrategy submitStrategy = getSubmitStrategy(queue);
        // 获取处理策略
        JnksIotRuleEngineProcessingStrategy ackStrategy = getProcessingStrategy(queue);
        submitStrategy.init(msgs);
        while (!stopped && !consumer.isStopped()) {
            JnksIotMsgPackProcessingContext packCtx = new JnksIotMsgPackProcessingContext(queue.getName(), submitStrategy, ackStrategy.isSkipTimeoutMsgs());
            // 提交消息到规则引擎进行处理
            submitStrategy.submitAttempt((id, msg) -> submitMessage(packCtx, id, msg));
            // 等待处理完成（带超时）
            final boolean timeout = !packCtx.await(queue.getPackProcessingTimeout(), TimeUnit.MILLISECONDS);
            // 分析处理结果
            JnksIotRuleEngineProcessingResult result = new JnksIotRuleEngineProcessingResult(queue.getName(), timeout, packCtx);
            // 如果超时则记录超时和失败的消息
            if (timeout) {
                printFirstOrAll(packCtx, packCtx.getPendingMap(), "Timeout");
            }
            if (!packCtx.getFailedMap().isEmpty()) {
                printFirstOrAll(packCtx, packCtx.getFailedMap(), "Failed");
            }
            packCtx.printProfilerStats();
            // 根据策略决定是否提交偏移量
            JnksIotRuleEngineProcessingDecision decision = ackStrategy.analyze(result);
            if (ctx.isStatsEnabled()) {
                stats.log(result, decision.isCommit());
            }

            packCtx.cleanup();
            // 如果决策是提交，则提交偏移量并退出循环
            if (decision.isCommit()) {
                submitStrategy.stop();
                consumer.commit();
                break;
            } else {
                // 如果需要重处理，更新提交策略并继续循环
                submitStrategy.update(decision.getReprocessMap());
            }
        }
    }

    private JnksIotRuleEngineSubmitStrategy getSubmitStrategy(Queue queue) {
        return ctx.getSubmitStrategyFactory().newInstance(queue.getName(), queue.getSubmitStrategy());
    }

    private JnksIotRuleEngineProcessingStrategy getProcessingStrategy(Queue queue) {
        return ctx.getProcessingStrategyFactory().newInstance(queue.getName(), queue.getProcessingStrategy());
    }

    private void submitMessage(JnksIotMsgPackProcessingContext packCtx, UUID id, JnksIotProtoQueueMsg<ToRuleEngineMsg> msg) {
        log.trace("[{}] Creating callback for topic {} message: {}", id, config.getName(), msg.getValue());
        ToRuleEngineMsg toRuleEngineMsg = msg.getValue();
        TenantId tenantId = TenantId.fromUUID(new UUID(toRuleEngineMsg.getTenantIdMSB(), toRuleEngineMsg.getTenantIdLSB()));
        JnksIotMsgCallback callback = ctx.isPrometheusStatsEnabled() ?
                new JnksIotMsgPackCallback(id, tenantId, packCtx, stats.getTimer(tenantId, SUCCESSFUL_STATUS), stats.getTimer(tenantId, FAILED_STATUS)) :
                new JnksIotMsgPackCallback(id, tenantId, packCtx);
        try {
            if (!toRuleEngineMsg.getJnksIotMsg().isEmpty()) {
                forwardToRuleEngineActor(config.getName(), tenantId, toRuleEngineMsg, callback);
            } else {
                callback.onSuccess();
            }
        } catch (Exception e) {
            callback.onFailure(new RuleEngineException(e.getMessage(), e));
        }
    }

    private void forwardToRuleEngineActor(String queueName, TenantId tenantId, ToRuleEngineMsg toRuleEngineMsg, JnksIotMsgCallback callback) {
        JnksIotMsg jnksIotMsg = JnksIotMsg.fromBytes(queueName, toRuleEngineMsg.getJnksIotMsg().toByteArray(), callback);
        QueueToRuleEngineMsg msg;
        ProtocolStringList relationTypesList = toRuleEngineMsg.getRelationTypesList();
        Set<String> relationTypes;
        if (relationTypesList.size() == 1) {
            relationTypes = Collections.singleton(relationTypesList.get(0));
        } else {
            relationTypes = new HashSet<>(relationTypesList);
        }
        msg = new QueueToRuleEngineMsg(tenantId, jnksIotMsg, relationTypes, toRuleEngineMsg.getFailureMessage());
        ctx.getActorContext().tell(msg);
    }

    private void printFirstOrAll(JnksIotMsgPackProcessingContext ctx, Map<UUID, JnksIotProtoQueueMsg<ToRuleEngineMsg>> map, String prefix) {
        boolean printAll = log.isTraceEnabled();
        log.info("[{}] {} to process [{}] messages", queueKey, prefix, map.size());
        for (Map.Entry<UUID, JnksIotProtoQueueMsg<ToRuleEngineMsg>> pending : map.entrySet()) {
            ToRuleEngineMsg tmp = pending.getValue().getValue();
            JnksIotMsg tmpMsg = JnksIotMsg.fromBytes(config.getName(), tmp.getJnksIotMsg().toByteArray(), JnksIotMsgCallback.EMPTY);
            RuleNodeInfo ruleNodeInfo = ctx.getLastVisitedRuleNode(pending.getKey());
            if (printAll) {
                log.trace("[{}][{}] {} to process message: {}, Last Rule Node: {}", queueKey, TenantId.fromUUID(new UUID(tmp.getTenantIdMSB(), tmp.getTenantIdLSB())), prefix, tmpMsg, ruleNodeInfo);
            } else {
                log.info("[{}] {} to process message: {}, Last Rule Node: {}", TenantId.fromUUID(new UUID(tmp.getTenantIdMSB(), tmp.getTenantIdLSB())), prefix, tmpMsg, ruleNodeInfo);
                break;
            }
        }
    }

    public void printStats(long ts) {
        stats.printStats();
        ctx.getStatisticsService().reportQueueStats(ts, stats);
        stats.reset();
    }

    private void drainQueue(List<JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToRuleEngineMsg>>> consumers) {
        long finishTs = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(ctx.getTopicDeletionDelayInSec());
        try {
            int n = 0;
            while (System.currentTimeMillis() <= finishTs) {
                for (JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToRuleEngineMsg>> consumer : consumers) {
                    List<JnksIotProtoQueueMsg<ToRuleEngineMsg>> msgs = consumer.poll(config.getPollInterval());
                    if (msgs.isEmpty()) {
                        continue;
                    }
                    for (JnksIotProtoQueueMsg<ToRuleEngineMsg> msg : msgs) {
                        try {
                            MsgProtos.JnksIotMsgProto jnksIotMsgProto = MsgProtos.JnksIotMsgProto.parseFrom(msg.getValue().getJnksIotMsg().toByteArray());
                            EntityId originator = EntityIdFactory.getByTypeAndUuid(jnksIotMsgProto.getEntityType(), new UUID(jnksIotMsgProto.getEntityIdMSB(), jnksIotMsgProto.getEntityIdLSB()));

                            TopicPartitionInfo tpi = ctx.getPartitionService().resolve(ServiceType.JNKS_IOT_RULE_ENGINE, config.getName(), TenantId.SYS_TENANT_ID, originator);
                            ctx.getProducerProvider().getRuleEngineMsgProducer().send(tpi, msg, null);
                            n++;
                        } catch (Throwable e) {
                            log.warn("Failed to move message to system {}: {}", consumer.getTopic(), msg, e);
                        }
                    }
                    consumer.commit();
                }
            }
            if (n > 0) {
                log.info("Moved {} messages from {} to system {}", n, queueKey, config.getName());
            }
        } catch (Exception e) {
            log.error("[{}] Failed to drain queue", queueKey, e);
        }
    }

}
