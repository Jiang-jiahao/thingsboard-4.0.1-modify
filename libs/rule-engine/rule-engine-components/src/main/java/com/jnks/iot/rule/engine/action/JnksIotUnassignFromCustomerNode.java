package com.jnks.iot.rule.engine.action;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.id.AssetId;
import com.jnks.iot.server.common.data.id.DashboardId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityViewId;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

@RuleNode(
        type = ComponentType.ACTION,
        name = "unassign from customer",
        configClazz = JnksIotUnassignFromCustomerNodeConfiguration.class,
        nodeDescription = "Unassign message originator entity from customer",
        nodeDetails = "If the message originator is not assigned to any customer, rule node will do nothing. <br><br>" +
                "If the incoming message originator is a dashboard, will try to search for the customer by title specified in the configuration. " +
                "If customer doesn't exist, the exception will be thrown. Otherwise will unassign the dashboard from retrieved customer.<br><br>" +
                "Other entities can be assigned only to one customer, so specified customer title in the configuration will be ignored if the originator isn't a dashboard.",
        configDirective = "jnksIotActionNodeUnAssignToCustomerConfig",
        icon = "remove_circle",
        version = 1
)
public class JnksIotUnassignFromCustomerNode extends JnksIotAbstractCustomerActionNode<JnksIotUnassignFromCustomerNodeConfiguration> {

    @Override
    protected boolean createCustomerIfNotExists() {
        return false;
    }

    @Override
    protected JnksIotUnassignFromCustomerNodeConfiguration loadCustomerNodeActionConfig(JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        return JnksIotNodeUtils.convert(configuration, JnksIotUnassignFromCustomerNodeConfiguration.class);
    }

    @Override
    protected ListenableFuture<Void> processCustomerAction(JnksIotContext ctx, JnksIotMsg msg) {
        var originator = msg.getOriginator();
        var originatorType = originator.getEntityType();
        var tenantId = ctx.getTenantId();
        if (EntityType.DASHBOARD.equals(originatorType)) {
            if (StringUtils.isEmpty(config.getCustomerNamePattern())) {
                throw new RuntimeException("Failed to unassign dashboard with id '" +
                        originator.getId() + "' from customer! Customer title should be specified!");
            }
            var customerIdFuture = getCustomerIdFuture(ctx, msg);
            return Futures.transform(customerIdFuture, customerId -> {
                ctx.getDashboardService().unassignDashboardFromCustomer(tenantId, new DashboardId(originator.getId()), customerId);
                return null;
            }, MoreExecutors.directExecutor());
        }
        return ctx.getDbCallbackExecutor().submit(() -> {
            switch (originatorType) {
                case ASSET ->
                        ctx.getAssetService().unassignAssetFromCustomer(tenantId, new AssetId(originator.getId()));
                case DEVICE ->
                        ctx.getDeviceService().unassignDeviceFromCustomer(tenantId, new DeviceId(originator.getId()));
                case ENTITY_VIEW ->
                        ctx.getEntityViewService().unassignEntityViewFromCustomer(tenantId, new EntityViewId(originator.getId()));
            }
            return null;
        });
    }

}
