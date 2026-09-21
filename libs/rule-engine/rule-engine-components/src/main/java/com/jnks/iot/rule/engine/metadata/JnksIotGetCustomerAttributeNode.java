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
import com.jnks.iot.rule.engine.util.EntitiesCustomerIdAsyncLoader;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.JnksIotPair;

@Slf4j
@RuleNode(
        type = ComponentType.ENRICHMENT,
        name = "customer attributes",
        configClazz = JnksIotGetEntityDataNodeConfiguration.class,
        version = 1,
        nodeDescription = "Adds message originator customer attributes or latest telemetry into message or message metadata",
        nodeDetails = "Useful in multi-customer solutions where each customer has a different configuration or threshold set " +
                "that is stored as customer attributes or telemetry data and used for dynamic message filtering, transformation, " +
                "or actions such as alarm creation if the threshold is exceeded.<br><br>" +
                "Output connections: <code>Success</code>, <code>Failure</code>.",
        configDirective = "jnksIotEnrichmentNodeCustomerAttributesConfig")
public class JnksIotGetCustomerAttributeNode extends JnksIotAbstractGetEntityDataNode<CustomerId> {

    private static final String CUSTOMER_NOT_FOUND_MESSAGE = "Failed to find customer for entity with id: %s and type: %s";

    @Override
    protected JnksIotGetEntityDataNodeConfiguration loadNodeConfiguration(JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        var config = JnksIotNodeUtils.convert(configuration, JnksIotGetEntityDataNodeConfiguration.class);
        checkIfMappingIsNotEmptyOrElseThrow(config.getDataMapping());
        checkDataToFetchSupportedOrElseThrow(config.getDataToFetch());
        return config;
    }

    @Override
    protected ListenableFuture<CustomerId> findEntityAsync(JnksIotContext ctx, EntityId originator) {
        return Futures.transformAsync(EntitiesCustomerIdAsyncLoader.findEntityIdAsync(ctx, originator),
                checkIfEntityIsPresentOrThrow(String.format(CUSTOMER_NOT_FOUND_MESSAGE, originator.getId(), originator.getEntityType().getNormalName())),
                ctx.getDbCallbackExecutor()
        );
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        return fromVersion == 0 ? upgradeToUseFetchToAndDataToFetch(oldConfiguration) : new JnksIotPair<>(false, oldConfiguration);
    }

}
