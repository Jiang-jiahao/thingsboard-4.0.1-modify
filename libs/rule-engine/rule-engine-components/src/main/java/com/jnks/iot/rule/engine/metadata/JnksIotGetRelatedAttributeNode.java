package com.jnks.iot.rule.engine.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.util.EntitiesRelatedEntityIdAsyncLoader;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.JnksIotPair;

import java.util.Arrays;

@Slf4j
@RuleNode(
        type = ComponentType.ENRICHMENT,
        name = "related entity data",
        configClazz = JnksIotGetRelatedDataNodeConfiguration.class,
        version = 1,
        nodeDescription = "Adds originators related entity attributes or latest telemetry or fields into message or message metadata",
        nodeDetails = "Related entity lookup based on the configured relation query. " +
                "If multiple related entities are found, only first entity is used for message enrichment, other entities are discarded. " +
                "Useful when you need to retrieve data from an entity that has a relation to the message originator and use them for further message processing.<br><br>" +
                "Output connections: <code>Success</code>, <code>Failure</code>.",
        configDirective = "jnksIotEnrichmentNodeRelatedAttributesConfig")
public class JnksIotGetRelatedAttributeNode extends JnksIotAbstractGetEntityDataNode<EntityId> {

    private static final String RELATED_ENTITY_NOT_FOUND_MESSAGE = "Failed to find related entity to message originator using relation query specified in the configuration!";

    @Override
    public JnksIotGetRelatedDataNodeConfiguration loadNodeConfiguration(JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        var config = JnksIotNodeUtils.convert(configuration, JnksIotGetRelatedDataNodeConfiguration.class);
        checkIfMappingIsNotEmptyOrElseThrow(config.getDataMapping());
        checkDataToFetchSupportedOrElseThrow(config.getDataToFetch());
        return config;
    }

    @Override
    public ListenableFuture<EntityId> findEntityAsync(JnksIotContext ctx, EntityId originator) {
        var relatedAttrConfig = (JnksIotGetRelatedDataNodeConfiguration) config;
        return Futures.transformAsync(
                EntitiesRelatedEntityIdAsyncLoader.findEntityAsync(ctx, originator, relatedAttrConfig.getRelationsQuery()),
                checkIfEntityIsPresentOrThrow(RELATED_ENTITY_NOT_FOUND_MESSAGE),
                ctx.getDbCallbackExecutor());
    }

    @Override
    protected void checkDataToFetchSupportedOrElseThrow(DataToFetch dataToFetch) throws JnksIotNodeException {
        if (dataToFetch == null) {
            throw new JnksIotNodeException("DataToFetch property cannot be null! Supported values are: " + Arrays.toString(DataToFetch.values()));
        }
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        return fromVersion == 0 ? upgradeToUseFetchToAndDataToFetch(oldConfiguration) : new JnksIotPair<>(false, oldConfiguration);
    }

}
