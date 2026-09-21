package com.jnks.iot.server.service.queue;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.common.data.id.RuleNodeId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.queue.RuleEngineException;
import com.jnks.iot.server.common.msg.queue.RuleNodeInfo;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.service.queue.processing.JnksIotRuleEngineSubmitStrategy;

import java.util.Comparator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 规则引擎消息包处理上下文，负责跟踪和管理一批消息的处理状态
 */
@Slf4j
public class JnksIotMsgPackProcessingContext {

    private final String queueName;
    private final JnksIotRuleEngineSubmitStrategy submitStrategy;
    private final boolean skipTimeoutMsgsPossible;
    @Getter
    private final boolean profilerEnabled;
    private final AtomicInteger pendingCount;
    private final CountDownLatch processingTimeoutLatch = new CountDownLatch(1);
    @Getter
    private final ConcurrentMap<UUID, JnksIotProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> pendingMap;
    @Getter
    private final ConcurrentMap<UUID, JnksIotProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> successMap = new ConcurrentHashMap<>();
    @Getter
    private final ConcurrentMap<UUID, JnksIotProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> failedMap = new ConcurrentHashMap<>();
    @Getter
    private final ConcurrentMap<TenantId, RuleEngineException> exceptionsMap = new ConcurrentHashMap<>();

    private final ConcurrentMap<UUID, RuleNodeInfo> lastRuleNodeMap = new ConcurrentHashMap<>();

    private volatile boolean canceled = false;

    public JnksIotMsgPackProcessingContext(String queueName, JnksIotRuleEngineSubmitStrategy submitStrategy, boolean skipTimeoutMsgsPossible) {
        this.queueName = queueName;
        this.submitStrategy = submitStrategy;
        this.skipTimeoutMsgsPossible = skipTimeoutMsgsPossible;
        this.profilerEnabled = log.isDebugEnabled();
        this.pendingMap = submitStrategy.getPendingMap();
        this.pendingCount = new AtomicInteger(pendingMap.size());
    }

    public boolean await(long packProcessingTimeout, TimeUnit milliseconds) throws InterruptedException {
        boolean success = processingTimeoutLatch.await(packProcessingTimeout, milliseconds);
        if (!success && profilerEnabled) {
            msgProfilerMap.values().forEach(this::onTimeout);
        }
        return success;
    }

    public void onSuccess(UUID id) {
        JnksIotProtoQueueMsg<TransportProtos.ToRuleEngineMsg> msg;
        boolean empty = false;
        msg = pendingMap.remove(id);
        if (msg != null) {
            empty = pendingCount.decrementAndGet() == 0;
            successMap.put(id, msg);
            submitStrategy.onSuccess(id);
        }
        if (empty) {
            processingTimeoutLatch.countDown();
        }
    }

    public void onFailure(TenantId tenantId, UUID id, RuleEngineException e) {
        JnksIotProtoQueueMsg<TransportProtos.ToRuleEngineMsg> msg;
        boolean empty = false;
        msg = pendingMap.remove(id);
        if (msg != null) {
            empty = pendingCount.decrementAndGet() == 0;
            failedMap.put(id, msg);
            exceptionsMap.putIfAbsent(tenantId, e);
        }
        if (empty) {
            processingTimeoutLatch.countDown();
        }
    }

    private final ConcurrentHashMap<UUID, JnksIotMsgProfilerInfo> msgProfilerMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, JnksIotRuleNodeProfilerInfo> ruleNodeProfilerMap = new ConcurrentHashMap<>();

    public void onProcessingStart(UUID id, RuleNodeInfo ruleNodeInfo) {
        lastRuleNodeMap.put(id, ruleNodeInfo);
        if (profilerEnabled) {
            msgProfilerMap.computeIfAbsent(id, JnksIotMsgProfilerInfo::new).onStart(ruleNodeInfo.getRuleNodeId());
            ruleNodeProfilerMap.putIfAbsent(ruleNodeInfo.getRuleNodeId().getId(), new JnksIotRuleNodeProfilerInfo(ruleNodeInfo));
        }
    }

    public void onProcessingEnd(UUID id, RuleNodeId ruleNodeId) {
        if (profilerEnabled) {
            long processingTime = msgProfilerMap.computeIfAbsent(id, JnksIotMsgProfilerInfo::new).onEnd(ruleNodeId);
            if (processingTime > 0) {
                ruleNodeProfilerMap.computeIfAbsent(ruleNodeId.getId(), JnksIotRuleNodeProfilerInfo::new).record(processingTime);
            }
        }
    }

    public void onTimeout(JnksIotMsgProfilerInfo profilerInfo) {
        Map.Entry<UUID, Long> ruleNodeInfo = profilerInfo.onTimeout();
        if (ruleNodeInfo != null) {
            ruleNodeProfilerMap.computeIfAbsent(ruleNodeInfo.getKey(), JnksIotRuleNodeProfilerInfo::new).record(ruleNodeInfo.getValue());
        }
    }

    public RuleNodeInfo getLastVisitedRuleNode(UUID id) {
        return lastRuleNodeMap.get(id);
    }

    public void printProfilerStats() {
        if (profilerEnabled) {
            log.debug("Top Rule Nodes by max execution time:");
            ruleNodeProfilerMap.values().stream()
                    .sorted(Comparator.comparingLong(JnksIotRuleNodeProfilerInfo::getMaxExecutionTime).reversed()).limit(5)
                    .forEach(info -> log.debug("[{}][{}] max execution time: {}. {}", queueName, info.getRuleNodeId(), info.getMaxExecutionTime(), info.getLabel()));

            log.info("Top Rule Nodes by avg execution time:");
            ruleNodeProfilerMap.values().stream()
                    .sorted(Comparator.comparingDouble(JnksIotRuleNodeProfilerInfo::getAvgExecutionTime).reversed()).limit(5)
                    .forEach(info -> log.info("[{}][{}] avg execution time: {}. {}", queueName, info.getRuleNodeId(), info.getAvgExecutionTime(), info.getLabel()));

            log.info("Top Rule Nodes by execution count:");
            ruleNodeProfilerMap.values().stream()
                    .sorted(Comparator.comparingInt(JnksIotRuleNodeProfilerInfo::getExecutionCount).reversed()).limit(5)
                    .forEach(info -> log.info("[{}][{}] execution count: {}. {}", queueName, info.getRuleNodeId(), info.getExecutionCount(), info.getLabel()));
        }
    }

    public void cleanup() {
        canceled = true;
        pendingMap.clear();
        successMap.clear();
        failedMap.clear();
    }

    public boolean isCanceled() {
        return skipTimeoutMsgsPossible && canceled;
    }
}
