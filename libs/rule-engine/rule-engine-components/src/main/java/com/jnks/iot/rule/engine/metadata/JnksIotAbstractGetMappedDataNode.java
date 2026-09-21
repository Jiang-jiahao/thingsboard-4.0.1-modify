package com.jnks.iot.rule.engine.metadata;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.util.EntitiesFieldsAsyncLoader;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.kv.KvEntry;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.jnks.iot.common.util.DonAsynchron.withCallback;

@Slf4j
public abstract class JnksIotAbstractGetMappedDataNode<T extends EntityId, C extends JnksIotGetMappedDataNodeConfiguration> extends JnksIotAbstractNodeWithFetchTo<C> {

    protected void checkIfMappingIsNotEmptyOrElseThrow(Map<String, String> dataMapping) throws JnksIotNodeException {
        if (dataMapping == null || dataMapping.isEmpty()) {
            throw new JnksIotNodeException("At least one mapping entry should be specified!");
        }
    }

    protected void processFieldsData(JnksIotContext ctx, JnksIotMsg msg, T entityId, ObjectNode msgDataAsJsonNode, boolean ignoreNullStrings) {
        var mappingsMap = processFieldsMappingPatterns(msg);
        withCallback(getEntityFieldsAsync(ctx, entityId, mappingsMap, ignoreNullStrings),
                data -> putFieldsDataAndTell(ctx, msg, msgDataAsJsonNode, data),
                t -> ctx.tellFailure(msg, t),
                MoreExecutors.directExecutor());
    }

    protected void processAttributesKvEntryData(JnksIotContext ctx, JnksIotMsg msg, T entityId, ObjectNode msgDataAsJsonNode) {
        var mappingsMap = processKvEntryMappingPatterns(msg);
        var sourceKeys = List.copyOf(mappingsMap.keySet());
        withCallback(getAttributesAsync(ctx, entityId, sourceKeys),
                data -> putKvEntryDataAndTell(ctx, msg, data, mappingsMap, msgDataAsJsonNode),
                t -> ctx.tellFailure(msg, t),
                MoreExecutors.directExecutor());
    }

    protected void processTsKvEntryData(JnksIotContext ctx, JnksIotMsg msg, T entityId, ObjectNode msgDataAsJsonNode) {
        var mappingsMap = processKvEntryMappingPatterns(msg);
        var sourceKeys = List.copyOf(mappingsMap.keySet());
        withCallback(getLatestTelemetryAsync(ctx, entityId, sourceKeys),
                data -> putKvEntryDataAndTell(ctx, msg, data, mappingsMap, msgDataAsJsonNode),
                t -> ctx.tellFailure(msg, t),
                MoreExecutors.directExecutor());
    }

    private void putFieldsDataAndTell(JnksIotContext ctx, JnksIotMsg msg, ObjectNode msgDataAsJsonNode, Map<String, String> targetKeysToSourceValuesMap) {
        JnksIotMsgMetaData msgMetaData = msg.getMetaData().copy();
        for (var entry : targetKeysToSourceValuesMap.entrySet()) {
            var targetKeyName = entry.getKey();
            var sourceFieldValue = entry.getValue();
            if (JnksIotMsgSource.DATA.equals(fetchTo)) {
                msgDataAsJsonNode.put(targetKeyName, sourceFieldValue);
            } else if (JnksIotMsgSource.METADATA.equals(fetchTo)) {
                msgMetaData.putValue(targetKeyName, sourceFieldValue);
            }
        }
        JnksIotMsg outMsg = transformMessage(msg, msgDataAsJsonNode, msgMetaData);
        ctx.tellSuccess(outMsg);
    }

    private void putKvEntryDataAndTell(JnksIotContext ctx, JnksIotMsg msg, List<? extends KvEntry> data, Map<String, String> map, ObjectNode msgData) {
        var msgMetaData = msg.getMetaData().copy();
        for (KvEntry entry : data) {
            String targetKey = map.get(entry.getKey());
            enrichMessage(msgData, msgMetaData, entry, targetKey);
        }
        ctx.tellSuccess(transformMessage(msg, msgData, msgMetaData));
    }

    private Map<String, String> processFieldsMappingPatterns(JnksIotMsg msg) {
        var mappingsMap = new HashMap<String, String>();
        config.getDataMapping().forEach((sourceField, targetKey) -> {
            String patternProcessedTargetKey = JnksIotNodeUtils.processPattern(targetKey, msg);
            mappingsMap.put(sourceField, patternProcessedTargetKey);
        });
        return mappingsMap;
    }

    private Map<String, String> processKvEntryMappingPatterns(JnksIotMsg msg) {
        var mappingsMap = new HashMap<String, String>();
        config.getDataMapping().forEach((sourceKey, targetKey) -> {
            String patternProcessedSourceKey = JnksIotNodeUtils.processPattern(sourceKey, msg);
            String patternProcessedTargetKey = JnksIotNodeUtils.processPattern(targetKey, msg);
            mappingsMap.put(patternProcessedSourceKey, patternProcessedTargetKey);
        });
        return mappingsMap;
    }

    private ListenableFuture<Map<String, String>> getEntityFieldsAsync(JnksIotContext ctx, EntityId entityId, Map<String, String> mappingsMap, boolean ignoreNullStrings) {
        return Futures.transform(EntitiesFieldsAsyncLoader.findAsync(ctx, entityId),
                fieldsData -> {
                    var targetKeysToSourceValuesMap = new HashMap<String, String>();
                    for (var mappingEntry : mappingsMap.entrySet()) {
                        var sourceFieldName = mappingEntry.getKey();
                        var targetKeyName = mappingEntry.getValue();
                        var sourceFieldValue = fieldsData.getFieldValue(sourceFieldName, ignoreNullStrings);
                        if (sourceFieldValue != null) {
                            targetKeysToSourceValuesMap.put(targetKeyName, sourceFieldValue);
                        }
                    }
                    return targetKeysToSourceValuesMap;
                }, ctx.getDbCallbackExecutor()
        );
    }

    private ListenableFuture<List<KvEntry>> getAttributesAsync(JnksIotContext ctx, EntityId entityId, List<String> attrKeys) {
        var latest = ctx.getAttributesService().find(ctx.getTenantId(), entityId, AttributeScope.SERVER_SCOPE, attrKeys);
        return Futures.transform(latest, l ->
                        l.stream()
                                .map(i -> (KvEntry) i)
                                .collect(Collectors.toList()),
                ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<List<KvEntry>> getLatestTelemetryAsync(JnksIotContext ctx, EntityId entityId, List<String> timeseriesKeys) {
        var latest = ctx.getTimeseriesService().findLatest(ctx.getTenantId(), entityId, timeseriesKeys);
        return Futures.transform(latest, l ->
                        l.stream()
                                .map(i -> (KvEntry) i)
                                .collect(Collectors.toList()),
                ctx.getDbCallbackExecutor());
    }

}
