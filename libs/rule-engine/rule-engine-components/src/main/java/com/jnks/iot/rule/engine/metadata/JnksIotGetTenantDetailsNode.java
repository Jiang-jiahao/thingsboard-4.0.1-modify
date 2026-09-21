package com.jnks.iot.rule.engine.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.Tenant;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;

@Slf4j
@RuleNode(type = ComponentType.ENRICHMENT,
        name = "tenant details",
        configClazz = JnksIotGetTenantDetailsNodeConfiguration.class,
        version = 1,
        nodeDescription = "Adds message originator tenant details into message or message metadata",
        nodeDetails = "Useful when we need to retrieve contact information from your tenant " +
                "such as email, phone, address, etc., for notifications via email, SMS, and other notification providers.<br><br>" +
                "Output connections: <code>Success</code>, <code>Failure</code>.",
        configDirective = "jnksIotEnrichmentNodeEntityDetailsConfig")
public class JnksIotGetTenantDetailsNode extends JnksIotAbstractGetEntityDetailsNode<JnksIotGetTenantDetailsNodeConfiguration, TenantId> {

    private static final String TENANT_PREFIX = "tenant_";

    @Override
    protected JnksIotGetTenantDetailsNodeConfiguration loadNodeConfiguration(JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        var config = JnksIotNodeUtils.convert(configuration, JnksIotGetTenantDetailsNodeConfiguration.class);
        checkIfDetailsListIsNotEmptyOrElseThrow(config.getDetailsList());
        return config;
    }

    @Override
    protected String getPrefix() {
        return TENANT_PREFIX;
    }

    @Override
    protected ListenableFuture<Tenant> getContactBasedFuture(JnksIotContext ctx, JnksIotMsg msg) {
        return ctx.getTenantService().findTenantByIdAsync(ctx.getTenantId(), ctx.getTenantId());
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        return fromVersion == 0 ?
                upgradeRuleNodesWithOldPropertyToUseFetchTo(
                        oldConfiguration,
                        "addToMetadata",
                        JnksIotMsgSource.METADATA.name(),
                        JnksIotMsgSource.DATA.name()) :
                new JnksIotPair<>(false, oldConfiguration);
    }

}
