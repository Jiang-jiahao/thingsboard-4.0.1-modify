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
import com.jnks.iot.rule.engine.util.EntitiesRelatedDeviceIdAsyncLoader;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;

@Slf4j
@RuleNode(type = ComponentType.ENRICHMENT,
        name = "related device attributes",
        configClazz = JnksIotGetDeviceAttrNodeConfiguration.class,
        version = 1,
        nodeDescription = "Add originators related device attributes and/or latest telemetry values into message or message metadata",
        nodeDetails = "Related device lookup based on the configured relation query. " +
                "If multiple related devices are found, only first device is used for message enrichment, other entities are discarded. " +
                "Useful when you need to retrieve attributes and/or latest telemetry values from device that has a relation " +
                "to the message originator and use them for further message processing.<br><br>" +
                "Output connections: <code>Success</code>, <code>Failure</code>.",
        configDirective = "jnksIotEnrichmentNodeDeviceAttributesConfig")
public class JnksIotGetDeviceAttrNode extends JnksIotAbstractGetAttributesNode<JnksIotGetDeviceAttrNodeConfiguration, DeviceId> {

    private static final String RELATED_DEVICE_NOT_FOUND_MESSAGE = "Failed to find related device to message originator using relation query specified in the configuration!";

    @Override
    protected JnksIotGetDeviceAttrNodeConfiguration loadNodeConfiguration(JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        return JnksIotNodeUtils.convert(configuration, JnksIotGetDeviceAttrNodeConfiguration.class);
    }

    @Override
    protected ListenableFuture<DeviceId> findEntityIdAsync(JnksIotContext ctx, JnksIotMsg msg) {
        return Futures.transformAsync(
                EntitiesRelatedDeviceIdAsyncLoader.findDeviceAsync(ctx, msg.getOriginator(), config.getDeviceRelationsQuery()),
                checkIfEntityIsPresentOrThrow(RELATED_DEVICE_NOT_FOUND_MESSAGE),
                ctx.getDbCallbackExecutor());
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
