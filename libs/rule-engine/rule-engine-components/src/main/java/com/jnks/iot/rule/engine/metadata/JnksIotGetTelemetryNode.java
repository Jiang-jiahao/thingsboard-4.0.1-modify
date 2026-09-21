package com.jnks.iot.rule.engine.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.math.NumberUtils;
import com.jnks.iot.common.util.DonAsynchron;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.kv.Aggregation;
import com.jnks.iot.server.common.data.kv.BaseReadTsKvQuery;
import com.jnks.iot.server.common.data.kv.ReadTsKvQuery;
import com.jnks.iot.server.common.data.kv.TsKvEntry;
import com.jnks.iot.server.common.data.page.SortOrder.Direction;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by mshvayka on 04.09.18.
 */
@Slf4j
@RuleNode(type = ComponentType.ENRICHMENT,
        name = "来源方遥测",
        configClazz = JnksIotGetTelemetryNodeConfiguration.class,
        version = 2,
        nodeDescription = "将所选时间范围内消息来源方的遥测数据添加到消息元数据中",
        nodeDetails = "当您需要获取消息来源方在特定时间范围内的遥测数据集时非常有用， " +
                "而不是仅获取最新的遥测，或者当您需要获取最接近获取区间起点或终点的遥测时。 " +
                "此外，该节点还可用于在配置的获取区间内聚合遥测。<br><br>" +
                "输出连接：<code>Success</code>、<code>Failure</code>。",
        configDirective = "jnksIotEnrichmentNodeGetTelemetryFromDatabase")
public class JnksIotGetTelemetryNode implements JnksIotNode {

    private JnksIotGetTelemetryNodeConfiguration config;
    private List<String> tsKeyNames;
    private int limit;
    private FetchMode fetchMode;
    private Direction orderBy;
    private Aggregation aggregation;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotGetTelemetryNodeConfiguration.class);
        tsKeyNames = config.getLatestTsKeyNames();
        if (tsKeyNames.isEmpty()) {
            throw new JnksIotNodeException("Telemetry should be specified!", true);
        }
        fetchMode = config.getFetchMode();
        if (fetchMode == null) {
            throw new JnksIotNodeException("FetchMode should be specified!", true);
        }
        switch (fetchMode) {
            case ALL:
                limit = validateLimit(config.getLimit());
                if (config.getOrderBy() == null) {
                    throw new JnksIotNodeException("OrderBy should be specified!", true);
                }
                orderBy = config.getOrderBy();
                if (config.getAggregation() == null) {
                    throw new JnksIotNodeException("Aggregation should be specified!", true);
                }
                aggregation = config.getAggregation();
                break;
            case FIRST:
                limit = 1;
                orderBy = Direction.ASC;
                aggregation = Aggregation.NONE;
                break;
            case LAST:
                limit = 1;
                orderBy = Direction.DESC;
                aggregation = Aggregation.NONE;
                break;
        }
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        Interval interval = getInterval(msg);
        if (interval.getStartTs() > interval.getEndTs()) {
            throw new RuntimeException("Interval start should be less than Interval end");
        }
        List<String> keys = JnksIotNodeUtils.processPatterns(tsKeyNames, msg);
        ListenableFuture<List<TsKvEntry>> list = ctx.getTimeseriesService().findAll(ctx.getTenantId(), msg.getOriginator(), buildQueries(interval, keys));
        DonAsynchron.withCallback(list, data -> {
            var metaData = updateMetadata(data, msg, keys);
            ctx.tellSuccess(msg.transform()
                    .metaData(metaData)
                    .build());
        }, error -> ctx.tellFailure(msg, error), ctx.getDbCallbackExecutor());
    }

    private List<ReadTsKvQuery> buildQueries(Interval interval, List<String> keys) {
        final long aggIntervalStep = Aggregation.NONE.equals(aggregation) ? 1 :
                // exact how it validates on BaseTimeseriesService.validate()
                // see CassandraBaseTimeseriesDao.findAllAsync()
                interval.getEndTs() - interval.getStartTs();

        return keys.stream()
                .map(key -> new BaseReadTsKvQuery(key, interval.getStartTs(), interval.getEndTs(), aggIntervalStep, limit, aggregation, orderBy.name()))
                .collect(Collectors.toList());
    }

    private JnksIotMsgMetaData updateMetadata(List<TsKvEntry> entries, JnksIotMsg msg, List<String> keys) {
        ObjectNode resultNode = JacksonUtil.newObjectNode(JacksonUtil.ALLOW_UNQUOTED_FIELD_NAMES_MAPPER);
        if (FetchMode.ALL.equals(fetchMode)) {
            entries.forEach(entry -> processArray(resultNode, entry));
        } else {
            entries.forEach(entry -> processSingle(resultNode, entry));
        }
        var copy = msg.getMetaData().copy();
        for (String key : keys) {
            if (resultNode.has(key)) {
                copy.putValue(key, resultNode.get(key).toString());
            }
        }
        return copy;
    }

    private void processSingle(ObjectNode node, TsKvEntry entry) {
        node.put(entry.getKey(), entry.getValueAsString());
    }

    private void processArray(ObjectNode node, TsKvEntry entry) {
        if (node.has(entry.getKey())) {
            ArrayNode arrayNode = (ArrayNode) node.get(entry.getKey());
            arrayNode.add(buildNode(entry));
        } else {
            ArrayNode arrayNode = JacksonUtil.ALLOW_UNQUOTED_FIELD_NAMES_MAPPER.createArrayNode();
            arrayNode.add(buildNode(entry));
            node.set(entry.getKey(), arrayNode);
        }
    }

    private ObjectNode buildNode(TsKvEntry entry) {
        ObjectNode obj = JacksonUtil.newObjectNode(JacksonUtil.ALLOW_UNQUOTED_FIELD_NAMES_MAPPER);
        obj.put("ts", entry.getTs());
        JacksonUtil.addKvEntry(obj, entry, "value", JacksonUtil.ALLOW_UNQUOTED_FIELD_NAMES_MAPPER);
        return obj;
    }

    private Interval getInterval(JnksIotMsg msg) {
        if (config.isUseMetadataIntervalPatterns()) {
            return getIntervalFromPatterns(msg);
        } else {
            Interval interval = new Interval();
            long ts = getCurrentTimeMillis();
            interval.setStartTs(ts - TimeUnit.valueOf(config.getStartIntervalTimeUnit()).toMillis(config.getStartInterval()));
            interval.setEndTs(ts - TimeUnit.valueOf(config.getEndIntervalTimeUnit()).toMillis(config.getEndInterval()));
            return interval;
        }
    }

    private Interval getIntervalFromPatterns(JnksIotMsg msg) {
        Interval interval = new Interval();
        interval.setStartTs(checkPattern(msg, config.getStartIntervalPattern()));
        interval.setEndTs(checkPattern(msg, config.getEndIntervalPattern()));
        return interval;
    }

    private long checkPattern(JnksIotMsg msg, String pattern) {
        String value = getValuePattern(msg, pattern);
        if (value == null) {
            throw new IllegalArgumentException("Message value: '" +
                    replaceRegex(pattern) + "' is undefined");
        }
        boolean parsable = NumberUtils.isParsable(value);
        if (!parsable) {
            throw new IllegalArgumentException("Message value: '" +
                    replaceRegex(pattern) + "' has invalid format");
        }
        return Long.parseLong(value);
    }

    private String getValuePattern(JnksIotMsg msg, String pattern) {
        String value = JnksIotNodeUtils.processPattern(pattern, msg);
        return value.equals(pattern) ? null : value;
    }

    private String replaceRegex(String pattern) {
        return pattern.replaceAll("[$\\[{}\\]]", "");
    }

    private int validateLimit(int limit) throws JnksIotNodeException {
        if (limit < 2 || limit > JnksIotGetTelemetryNodeConfiguration.MAX_FETCH_SIZE) {
            throw new JnksIotNodeException("Limit should be in a range from 2 to 1000.", true);
        }
        return limit;
    }

    long getCurrentTimeMillis() {
        return System.currentTimeMillis();
    }

    @Data
    @NoArgsConstructor
    private static class Interval {
        private Long startTs;
        private Long endTs;
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        boolean hasChanges = false;
        switch (fromVersion) {
            case 0: {
                if (oldConfiguration.hasNonNull("fetchMode")) {
                    String fetchMode = oldConfiguration.get("fetchMode").asText();
                    switch (fetchMode) {
                        case "FIRST" -> {
                            ((ObjectNode) oldConfiguration).put("orderBy", Direction.ASC.name());
                            ((ObjectNode) oldConfiguration).put("aggregation", Aggregation.NONE.name());
                            hasChanges = true;
                        }
                        case "LAST" -> {
                            ((ObjectNode) oldConfiguration).put("orderBy", Direction.DESC.name());
                            ((ObjectNode) oldConfiguration).put("aggregation", Aggregation.NONE.name());
                            hasChanges = true;
                        }
                        case "ALL" -> {
                            if (oldConfiguration.has("orderBy") &&
                                    (oldConfiguration.get("orderBy").isNull() || oldConfiguration.get("orderBy").asText().isEmpty())) {
                                ((ObjectNode) oldConfiguration).put("orderBy", Direction.ASC.name());
                                hasChanges = true;
                            }
                            if (oldConfiguration.has("aggregation") &&
                                    (oldConfiguration.get("aggregation").isNull() || oldConfiguration.get("aggregation").asText().isEmpty())) {
                                ((ObjectNode) oldConfiguration).put("aggregation", Aggregation.NONE.name());
                                hasChanges = true;
                            }
                        }
                        default -> {
                            ((ObjectNode) oldConfiguration).put("fetchMode", FetchMode.LAST.name());
                            ((ObjectNode) oldConfiguration).put("orderBy", Direction.DESC.name());
                            ((ObjectNode) oldConfiguration).put("aggregation", Aggregation.NONE.name());
                            hasChanges = true;
                        }
                    }
                }
            }
            case 1: {
                if (!oldConfiguration.hasNonNull("limit")) {
                    ((ObjectNode) oldConfiguration).put("limit", 1000);
                    hasChanges = true;
                }
                if (oldConfiguration.has("fetchMode") && oldConfiguration.get("fetchMode").asText().equals("ALL")) {
                    if (!oldConfiguration.hasNonNull("aggregation")) {
                        ((ObjectNode) oldConfiguration).put("aggregation", Aggregation.NONE.name());
                        hasChanges = true;
                    }
                    if (!oldConfiguration.hasNonNull("orderBy")) {
                        ((ObjectNode) oldConfiguration).put("orderBy", Direction.ASC.name());
                        hasChanges = true;
                    }
                }
                break;
            }
        }
        return new JnksIotPair<>(hasChanges, oldConfiguration);
    }

}
