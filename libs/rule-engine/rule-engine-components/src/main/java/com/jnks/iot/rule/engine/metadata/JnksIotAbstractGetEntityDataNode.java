package com.jnks.iot.rule.engine.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import static com.jnks.iot.common.util.DonAsynchron.withCallback;

@Slf4j
public abstract class JnksIotAbstractGetEntityDataNode<T extends EntityId> extends JnksIotAbstractGetMappedDataNode<T, JnksIotGetEntityDataNodeConfiguration> {

    private final static String DATA_TO_FETCH_PROPERTY_NAME = "dataToFetch";
    private static final String OLD_DATA_TO_FETCH_PROPERTY_NAME = "telemetry";
    private final static String DATA_MAPPING_PROPERTY_NAME = "dataMapping";
    private static final String OLD_DATA_MAPPING_PROPERTY_NAME = "attrMapping";

    private static final String DATA_TO_FETCH_VALIDATION_MSG = "DataToFetch property has invalid value: %s." +
            " Only ATTRIBUTES and LATEST_TELEMETRY values supported!";

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        var msgDataAsObjectNode = JnksIotMsgSource.DATA.equals(fetchTo) ? getMsgDataAsObjectNode(msg) : null;
        withCallback(findEntityAsync(ctx, msg.getOriginator()),
                entityId -> processDataAndTell(ctx, msg, entityId, msgDataAsObjectNode),
                t -> ctx.tellFailure(msg, t), ctx.getDbCallbackExecutor());
    }

    protected abstract ListenableFuture<T> findEntityAsync(JnksIotContext ctx, EntityId originator);

    protected void checkDataToFetchSupportedOrElseThrow(DataToFetch dataToFetch) throws JnksIotNodeException {
        if (dataToFetch == null || dataToFetch.equals(DataToFetch.FIELDS)) {
            throw new JnksIotNodeException(String.format(DATA_TO_FETCH_VALIDATION_MSG, dataToFetch));
        }
    }

    protected void processDataAndTell(JnksIotContext ctx, JnksIotMsg msg, T entityId, ObjectNode msgDataAsJsonNode) {
        DataToFetch dataToFetch = config.getDataToFetch();
        switch (dataToFetch) {
            case ATTRIBUTES:
                processAttributesKvEntryData(ctx, msg, entityId, msgDataAsJsonNode);
                break;
            case LATEST_TELEMETRY:
                processTsKvEntryData(ctx, msg, entityId, msgDataAsJsonNode);
                break;
            case FIELDS:
                processFieldsData(ctx, msg, entityId, msgDataAsJsonNode, true);
                break;
        }
    }

    protected JnksIotPair<Boolean, JsonNode> upgradeToUseFetchToAndDataToFetch(JsonNode oldConfiguration) throws JnksIotNodeException {
        var newConfigObjectNode = (ObjectNode) oldConfiguration;
        if (!newConfigObjectNode.has(OLD_DATA_TO_FETCH_PROPERTY_NAME)) {
            throw new JnksIotNodeException("property to update: '" + OLD_DATA_TO_FETCH_PROPERTY_NAME + "' doesn't exists in configuration!");
        }
        if (!newConfigObjectNode.has(OLD_DATA_MAPPING_PROPERTY_NAME)) {
            throw new JnksIotNodeException("property to update: '" + OLD_DATA_MAPPING_PROPERTY_NAME + "' doesn't exists in configuration!");
        }
        newConfigObjectNode.set(DATA_MAPPING_PROPERTY_NAME, newConfigObjectNode.get(OLD_DATA_MAPPING_PROPERTY_NAME));
        newConfigObjectNode.remove(OLD_DATA_MAPPING_PROPERTY_NAME);
        var value = newConfigObjectNode.get(OLD_DATA_TO_FETCH_PROPERTY_NAME).asText();
        if ("true".equals(value)) {
            newConfigObjectNode.remove(OLD_DATA_TO_FETCH_PROPERTY_NAME);
            newConfigObjectNode.put(DATA_TO_FETCH_PROPERTY_NAME, DataToFetch.LATEST_TELEMETRY.name());
        } else if ("false".equals(value)) {
            newConfigObjectNode.remove(OLD_DATA_TO_FETCH_PROPERTY_NAME);
            newConfigObjectNode.put(DATA_TO_FETCH_PROPERTY_NAME, DataToFetch.ATTRIBUTES.name());
        } else {
            throw new JnksIotNodeException("property to update: '" + OLD_DATA_TO_FETCH_PROPERTY_NAME + "' has unexpected value: " + value + ". Allowed values: true or false!");
        }
        newConfigObjectNode.put(FETCH_TO_PROPERTY_NAME, JnksIotMsgSource.METADATA.name());
        return new JnksIotPair<>(true, newConfigObjectNode);
    }

}
