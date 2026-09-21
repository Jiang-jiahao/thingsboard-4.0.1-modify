package com.jnks.iot.rule.engine.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ConcurrentReferenceHashMap;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.util.SemaphoreWithJnksIotMsgQueue;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.kv.TsKvEntry;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

@Slf4j
@RuleNode(type = ComponentType.ENRICHMENT,
        name = "计算增量",
        version = 1,
        relationTypes = {JnksIotNodeConnectionType.SUCCESS, JnksIotNodeConnectionType.FAILURE, JnksIotNodeConnectionType.OTHER},
        configClazz = CalculateDeltaNodeConfiguration.class,
        nodeDescription = "计算此前时序数据键读数与 " +
                "传入消息中该键当前值之间的增量和经过的时间",
        nodeDetails = "适用于计量场景，此时你需要根据脉冲计数器读数计算消耗量。<br><br>" +
                "输出连接：<code>Success</code>、<code>Other</code> 或 <code>Failure</code>。",
        configDirective = "jnksIotEnrichmentNodeCalculateDeltaConfig")
public class CalculateDeltaNode implements JnksIotNode {

    private Map<EntityId, ValueWithTs> cache;
    private Map<EntityId, SemaphoreWithJnksIotMsgQueue> locks;

    private CalculateDeltaNodeConfiguration config;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, CalculateDeltaNodeConfiguration.class);
        if (StringUtils.isBlank(config.getInputValueKey())) {
            throw new JnksIotNodeException("Input value key should be specified!", true);
        }
        if (StringUtils.isBlank(config.getOutputValueKey())) {
            throw new JnksIotNodeException("Output value key should be specified!", true);
        }
        if (config.isAddPeriodBetweenMsgs() && StringUtils.isBlank(config.getPeriodValueKey())) {
            throw new JnksIotNodeException("Period value key should be specified!", true);
        }
        locks = new ConcurrentReferenceHashMap<>(16, ConcurrentReferenceHashMap.ReferenceType.WEAK);
        if (config.isUseCache()) {
            cache = new ConcurrentReferenceHashMap<>(16, ConcurrentReferenceHashMap.ReferenceType.SOFT);
        }
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        if (!msg.isTypeOf(JnksIotMsgType.POST_TELEMETRY_REQUEST)) {
            ctx.tellNext(msg, JnksIotNodeConnectionType.OTHER);
            return;
        }
        JsonNode msgData = JacksonUtil.toJsonNode(msg.getData());
        if (msgData == null || !msgData.has(config.getInputValueKey())) {
            ctx.tellNext(msg, JnksIotNodeConnectionType.OTHER);
            return;
        }
        locks.computeIfAbsent(msg.getOriginator(), SemaphoreWithJnksIotMsgQueue::new)
                .addToQueueAndTryProcess(msg, ctx, this::processMsgAsync);
    }

    @Override
    public void destroy() {
        locks.clear();
        if (config.isUseCache()) {
            cache.clear();
        }
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        boolean hasChanges = false;
        switch (fromVersion) {
            case 0:
                String excludeZeroDeltas = "excludeZeroDeltas";
                if (!oldConfiguration.has(excludeZeroDeltas)) {
                    hasChanges = true;
                    ((ObjectNode) oldConfiguration).put(excludeZeroDeltas, false);
                }
                break;
            default:
                break;
        }
        return new JnksIotPair<>(hasChanges, oldConfiguration);
    }

    private ListenableFuture<ValueWithTs> fetchLatestValueAsync(JnksIotContext ctx, EntityId entityId) {
        return Futures.transform(ctx.getTimeseriesService().findLatest(ctx.getTenantId(), entityId, config.getInputValueKey()),
                tsKvEntryOpt -> tsKvEntryOpt.map(this::extractValue).orElse(null), MoreExecutors.directExecutor());
    }

    private ValueWithTs extractValue(TsKvEntry kvEntry) {
        if (kvEntry == null || kvEntry.getValue() == null) {
            return null;
        }
        double result = 0.0;
        long ts = kvEntry.getTs();
        switch (kvEntry.getDataType()) {
            case LONG -> result = kvEntry.getLongValue().get();
            case DOUBLE -> result = kvEntry.getDoubleValue().get();
            case STRING -> {
                try {
                    result = Double.parseDouble(kvEntry.getStrValue().get());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Calculation failed. Unable to parse value [" + kvEntry.getStrValue().get() + "]" +
                            " of telemetry [" + kvEntry.getKey() + "] to Double");
                }
            }
            case BOOLEAN -> throw new IllegalArgumentException("Calculation failed. Boolean values are not supported!");
            case JSON -> throw new IllegalArgumentException("Calculation failed. JSON values are not supported!");
        }
        return new ValueWithTs(ts, result);
    }

    protected ListenableFuture<JnksIotMsg> processMsgAsync(JnksIotContext ctx, JnksIotMsg msg) {
        ListenableFuture<ValueWithTs> latestValueFuture = getLatestFromCacheOrFetchFromDb(ctx, msg);
        return Futures.transform(latestValueFuture, previousData -> {
            ObjectNode json = (ObjectNode) JacksonUtil.toJsonNode(msg.getData());
            double currentValue = json.get(config.getInputValueKey()).asDouble();
            if (config.isUseCache()) {
                cache.put(msg.getOriginator(), new ValueWithTs(msg.getMetaDataTs(), currentValue));
            }
            BigDecimal delta = BigDecimal.valueOf(previousData != null ? currentValue - previousData.value : 0.0);
            if (config.isTellFailureIfDeltaIsNegative() && delta.doubleValue() < 0) {
                throw new IllegalArgumentException("Delta value is negative!");
            }
            if (config.isExcludeZeroDeltas() && delta.doubleValue() == 0) {
                return msg;
            }
            if (config.getRound() != null) {
                delta = delta.setScale(config.getRound(), RoundingMode.HALF_UP);
            }
            if (delta.stripTrailingZeros().scale() > 0) {
                json.put(config.getOutputValueKey(), delta.doubleValue());
            } else {
                json.put(config.getOutputValueKey(), delta.longValueExact());
            }
            if (config.isAddPeriodBetweenMsgs()) {
                long period = previousData != null ? msg.getMetaDataTs() - previousData.ts : 0;
                json.put(config.getPeriodValueKey(), period);
            }
            return msg.transform()
                    .data(JacksonUtil.toString(json))
                    .build();
        }, MoreExecutors.directExecutor());
    }

    private ListenableFuture<ValueWithTs> getLatestFromCacheOrFetchFromDb(JnksIotContext ctx, JnksIotMsg msg) {
        EntityId originator = msg.getOriginator();
        if (config.isUseCache()) {
            ValueWithTs valueWithTs = cache.get(msg.getOriginator());
            return valueWithTs != null ? Futures.immediateFuture(valueWithTs) : fetchLatestValueAsync(ctx, originator);
        }
        return fetchLatestValueAsync(ctx, originator);
    }

    private record ValueWithTs(long ts, double value) {
    }

}
