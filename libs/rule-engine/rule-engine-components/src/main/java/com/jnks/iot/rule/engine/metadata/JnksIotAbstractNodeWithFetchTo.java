package com.jnks.iot.rule.engine.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.kv.KvEntry;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.Arrays;
import java.util.NoSuchElementException;

@Slf4j
public abstract class JnksIotAbstractNodeWithFetchTo<C extends JnksIotAbstractFetchToNodeConfiguration> implements JnksIotNode {

    protected final static String FETCH_TO_PROPERTY_NAME = "fetchTo";

    protected C config;
    protected JnksIotMsgSource fetchTo;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        config = loadNodeConfiguration(configuration);
        if (config.getFetchTo() == null) {
            throw new JnksIotNodeException("FetchTo option can't be null! Allowed values: " + Arrays.toString(JnksIotMsgSource.values()));
        }
        fetchTo = config.getFetchTo();
    }

    protected abstract C loadNodeConfiguration(JnksIotNodeConfiguration configuration) throws JnksIotNodeException;

    protected <I extends EntityId> AsyncFunction<I, I> checkIfEntityIsPresentOrThrow(String message) {
        return id -> {
            if (id == null || id.isNullUid()) {
                return Futures.immediateFailedFuture(new NoSuchElementException(message));
            }
            return Futures.immediateFuture(id);
        };
    }

    protected ObjectNode getMsgDataAsObjectNode(JnksIotMsg msg) {
        var msgDataNode = JacksonUtil.toJsonNode(msg.getData());
        if (msgDataNode == null || !msgDataNode.isObject()) {
            throw new IllegalArgumentException("Message body is not an object!");
        }
        return (ObjectNode) msgDataNode;
    }

    protected void enrichMessage(ObjectNode msgData, JnksIotMsgMetaData metaData, KvEntry kvEntry, String targetKey) {
        if (JnksIotMsgSource.DATA.equals(fetchTo)) {
            JacksonUtil.addKvEntry(msgData, kvEntry, targetKey);
        } else if (JnksIotMsgSource.METADATA.equals(fetchTo)) {
            metaData.putValue(targetKey, kvEntry.getValueAsString());
        }
    }

    protected JnksIotMsg transformMessage(JnksIotMsg msg, ObjectNode msgDataNode, JnksIotMsgMetaData msgMetaData) {
        switch (fetchTo) {
            case DATA:
                return msg.transform()
                        .data(JacksonUtil.toString(msgDataNode))
                        .build();
            case METADATA:
                return msg.transform()
                        .metaData(msgMetaData)
                        .build();
            default:
                log.debug("Unexpected FetchTo value: {}. Allowed values: {}", fetchTo, JnksIotMsgSource.values());
                return msg;
        }
    }

    protected JnksIotPair<Boolean, JsonNode> upgradeRuleNodesWithOldPropertyToUseFetchTo(
            JsonNode oldConfiguration,
            String oldProperty,
            String ifTrue,
            String ifFalse
    ) throws JnksIotNodeException {
        var newConfig = (ObjectNode) oldConfiguration;
        if (!newConfig.has(oldProperty)) {
            throw new JnksIotNodeException("property to update: '" + oldProperty + "' doesn't exists in configuration!");
        }
        return upgradeConfigurationToUseFetchTo(oldProperty, ifTrue, ifFalse, newConfig);
    }

    protected JnksIotPair<Boolean, JsonNode> upgradeConfigurationToUseFetchTo(
            String oldProperty, String ifTrue,
            String ifFalse, ObjectNode newConfig
    ) throws JnksIotNodeException {
        var value = newConfig.get(oldProperty).asText();
        if ("true".equals(value)) {
            newConfig.remove(oldProperty);
            newConfig.put(FETCH_TO_PROPERTY_NAME, ifTrue);
            return new JnksIotPair<>(true, newConfig);
        } else if ("false".equals(value)) {
            newConfig.remove(oldProperty);
            newConfig.put(FETCH_TO_PROPERTY_NAME, ifFalse);
            return new JnksIotPair<>(true, newConfig);
        } else {
            throw new JnksIotNodeException("property to update: '" + oldProperty + "' has unexpected value: "
                    + value + ". Allowed values: true or false!");
        }
    }

}
