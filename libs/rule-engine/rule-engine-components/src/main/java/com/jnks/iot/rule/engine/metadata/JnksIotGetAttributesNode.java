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
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;

/**
 * Created by ashvayka on 19.01.18.
 */
@Slf4j
@RuleNode(type = ComponentType.ENRICHMENT,
        name = "originator attributes",
        configClazz = JnksIotGetAttributesNodeConfiguration.class,
        version = 1,
        nodeDescription = "Adds attributes and/or latest timeseries data for the message originator to the message or message metadata",
        nodeDetails = "Useful when you need to retrieve some attributes or the latest telemetry readings from the message originator " +
                "that are not included in the incoming message to use them for further message processing. " +
                "For example to filter messages based on the threshold value stored in the attributes.<br><br>" +
                "Output connections: <code>Success</code>, <code>Failure</code>.",
        configDirective = "jnksIotEnrichmentNodeOriginatorAttributesConfig")
public class JnksIotGetAttributesNode extends JnksIotAbstractGetAttributesNode<JnksIotGetAttributesNodeConfiguration, EntityId> {

    @Override
    protected JnksIotGetAttributesNodeConfiguration loadNodeConfiguration(JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        return JnksIotNodeUtils.convert(configuration, JnksIotGetAttributesNodeConfiguration.class);
    }

    @Override
    protected ListenableFuture<EntityId> findEntityIdAsync(JnksIotContext ctx, JnksIotMsg msg) {
        return Futures.immediateFuture(msg.getOriginator());
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        return fromVersion == 0 ?
                upgradeRuleNodesWithOldPropertyToUseFetchTo(
                        oldConfiguration,
                        "fetchToData",
                        JnksIotMsgSource.DATA.name(),
                        JnksIotMsgSource.METADATA.name()) :
                new JnksIotPair<>(false, oldConfiguration);
    }

}
