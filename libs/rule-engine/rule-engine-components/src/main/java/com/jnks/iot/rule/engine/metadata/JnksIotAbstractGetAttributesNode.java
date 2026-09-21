package com.jnks.iot.rule.engine.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;
import com.jnks.iot.server.common.data.kv.BasicTsKvEntry;
import com.jnks.iot.server.common.data.kv.JsonDataEntry;
import com.jnks.iot.server.common.data.kv.KvEntry;
import com.jnks.iot.server.common.data.kv.TsKvEntry;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.jnks.iot.common.util.DonAsynchron.withCallback;
import static com.jnks.iot.server.common.data.DataConstants.CLIENT_SCOPE;
import static com.jnks.iot.server.common.data.DataConstants.LATEST_TS;
import static com.jnks.iot.server.common.data.DataConstants.SERVER_SCOPE;
import static com.jnks.iot.server.common.data.DataConstants.SHARED_SCOPE;

@Slf4j
public abstract class JnksIotAbstractGetAttributesNode<C extends JnksIotGetAttributesNodeConfiguration, T extends EntityId> extends JnksIotAbstractNodeWithFetchTo<C> {

    private static final String VALUE = "value";
    private static final String TS = "ts";
    private boolean isTellFailureIfAbsent;
    private boolean getLatestValueWithTs;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        super.init(ctx, configuration);
        getLatestValueWithTs = config.isGetLatestValueWithTs();
        isTellFailureIfAbsent = BooleanUtils.toBooleanDefaultIfNull(config.isTellFailureIfAbsent(), true);
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) throws JnksIotNodeException {
        var msgDataAsObjectNode = JnksIotMsgSource.DATA.equals(fetchTo) ? getMsgDataAsObjectNode(msg) : null;
        withCallback(
                findEntityIdAsync(ctx, msg),
                entityId -> safePutAttributes(ctx, msg, msgDataAsObjectNode, entityId),
                t -> ctx.tellFailure(msg, t), ctx.getDbCallbackExecutor());
    }

    protected abstract ListenableFuture<T> findEntityIdAsync(JnksIotContext ctx, JnksIotMsg msg);

    protected JnksIotPair<Boolean, JsonNode> upgradeRuleNodesWithOldPropertyToUseFetchTo(
            JsonNode oldConfiguration,
            String oldProperty,
            String ifTrue,
            String ifFalse
    ) throws JnksIotNodeException {
        var newConfigObjectNode = (ObjectNode) oldConfiguration;
        if (!newConfigObjectNode.has(oldProperty)) {
            newConfigObjectNode.put(FETCH_TO_PROPERTY_NAME, JnksIotMsgSource.METADATA.name());
            return new JnksIotPair<>(true, newConfigObjectNode);
        }
        if (newConfigObjectNode.get(oldProperty).isNull()) {
            newConfigObjectNode.remove(oldProperty);
            newConfigObjectNode.put(FETCH_TO_PROPERTY_NAME, JnksIotMsgSource.METADATA.name());
            return new JnksIotPair<>(true, newConfigObjectNode);
        }
        return upgradeConfigurationToUseFetchTo(oldProperty, ifTrue, ifFalse, newConfigObjectNode);
    }

    private void safePutAttributes(JnksIotContext ctx, JnksIotMsg msg, ObjectNode msgDataNode, T entityId) {
        Set<JnksIotPair<String, List<String>>> failuresPairSet = ConcurrentHashMap.newKeySet();
        var getKvEntryPairFutures = Futures.allAsList(
                getAttrAsync(ctx, entityId, AttributeScope.CLIENT_SCOPE, JnksIotNodeUtils.processPatterns(config.getClientAttributeNames(), msg), failuresPairSet),
                getAttrAsync(ctx, entityId, AttributeScope.SHARED_SCOPE, JnksIotNodeUtils.processPatterns(config.getSharedAttributeNames(), msg), failuresPairSet),
                getAttrAsync(ctx, entityId, AttributeScope.SERVER_SCOPE, JnksIotNodeUtils.processPatterns(config.getServerAttributeNames(), msg), failuresPairSet),
                getLatestTelemetry(ctx, entityId, JnksIotNodeUtils.processPatterns(config.getLatestTsKeyNames(), msg), failuresPairSet)
        );
        withCallback(getKvEntryPairFutures, futuresList -> {
            var msgMetaData = msg.getMetaData().copy();
            futuresList.stream().filter(Objects::nonNull).forEach(kvEntriesPair -> {
                var keyScope = kvEntriesPair.getFirst();
                var kvEntryList = kvEntriesPair.getSecond();
                var prefix = getPrefix(keyScope);
                kvEntryList.forEach(kvEntry -> {
                    String targetKey = prefix + kvEntry.getKey();
                    enrichMessage(msgDataNode, msgMetaData, kvEntry, targetKey);
                });
            });
            JnksIotMsg outMsg = transformMessage(msg, msgDataNode, msgMetaData);
            if (failuresPairSet.isEmpty()) {
                ctx.tellSuccess(outMsg);
            } else {
                ctx.tellFailure(outMsg, reportFailures(failuresPairSet));
            }
        }, t -> ctx.tellFailure(msg, t), MoreExecutors.directExecutor());
    }

    private ListenableFuture<JnksIotPair<String, List<AttributeKvEntry>>> getAttrAsync(
            JnksIotContext ctx,
            EntityId entityId,
            AttributeScope scope,
            List<String> keys,
            Set<JnksIotPair<String, List<String>>> failuresPairSet
    ) {
        if (CollectionUtils.isEmpty(keys)) {
            return Futures.immediateFuture(null);
        }
        var attributeKvEntryListFuture = ctx.getAttributesService().find(ctx.getTenantId(), entityId, scope, keys);
        return Futures.transform(attributeKvEntryListFuture, attributeKvEntryList -> {
            if (isTellFailureIfAbsent && attributeKvEntryList.size() != keys.size()) {
                List<String> nonExistentKeys = getNonExistentKeys(attributeKvEntryList, keys);
                failuresPairSet.add(new JnksIotPair<>(scope.name(), nonExistentKeys));
            }
            return new JnksIotPair<>(scope.name(), attributeKvEntryList);
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<JnksIotPair<String, List<TsKvEntry>>> getLatestTelemetry(JnksIotContext ctx, EntityId entityId, List<String> keys, Set<JnksIotPair<String, List<String>>> failuresPairSet) {
        if (CollectionUtils.isEmpty(keys)) {
            return Futures.immediateFuture(null);
        }
        ListenableFuture<List<TsKvEntry>> latestTelemetryFutures = ctx.getTimeseriesService().findLatest(ctx.getTenantId(), entityId, keys);
        return Futures.transform(latestTelemetryFutures, tsKvEntries -> {
            var listTsKvEntry = new ArrayList<TsKvEntry>();
            var nonExistentKeys = new ArrayList<String>();
            tsKvEntries.forEach(tsKvEntry -> {
                if (tsKvEntry.getValue() == null) {
                    if (isTellFailureIfAbsent) {
                        nonExistentKeys.add(tsKvEntry.getKey());
                    }
                } else if (getLatestValueWithTs) {
                    listTsKvEntry.add(getValueWithTs(tsKvEntry));
                } else {
                    listTsKvEntry.add(new BasicTsKvEntry(tsKvEntry.getTs(), tsKvEntry));
                }
            });
            if (isTellFailureIfAbsent && !nonExistentKeys.isEmpty()) {
                failuresPairSet.add(new JnksIotPair<>(LATEST_TS, nonExistentKeys));
            }
            return new JnksIotPair<>(LATEST_TS, listTsKvEntry);
        }, ctx.getDbCallbackExecutor());
    }

    private TsKvEntry getValueWithTs(TsKvEntry tsKvEntry) {
        var mapper = JnksIotMsgSource.DATA.equals(fetchTo) ? JacksonUtil.OBJECT_MAPPER : JacksonUtil.ALLOW_UNQUOTED_FIELD_NAMES_MAPPER;
        var value = JacksonUtil.newObjectNode(mapper);
        value.put(TS, tsKvEntry.getTs());
        JacksonUtil.addKvEntry(value, tsKvEntry, VALUE, mapper);
        return new BasicTsKvEntry(tsKvEntry.getTs(), new JsonDataEntry(tsKvEntry.getKey(), value.toString()));
    }

    private String getPrefix(String scope) {
        var prefix = "";
        switch (scope) {
            case CLIENT_SCOPE:
                prefix = "cs_";
                break;
            case SHARED_SCOPE:
                prefix = "shared_";
                break;
            case SERVER_SCOPE:
                prefix = "ss_";
                break;
        }
        return prefix;
    }

    private List<String> getNonExistentKeys(List<AttributeKvEntry> existingAttributesKvEntry, List<String> allKeys) {
        List<String> existingKeys = existingAttributesKvEntry.stream().map(KvEntry::getKey).collect(Collectors.toList());
        return allKeys.stream().filter(key -> !existingKeys.contains(key)).collect(Collectors.toList());
    }

    private RuntimeException reportFailures(Set<JnksIotPair<String, List<String>>> failuresPairSet) {
        var errorMessage = new StringBuilder("The following attribute/telemetry keys is not present in the DB: ").append("\n");
        failuresPairSet.forEach(failurePair -> {
            String scope = failurePair.getFirst();
            List<String> nonExistentKeys = failurePair.getSecond();
            errorMessage.append("\t").append("[").append(scope).append("]:").append(nonExistentKeys.toString()).append("\n");
        });
        failuresPairSet.clear();
        return new RuntimeException(errorMessage.toString());
    }

}
