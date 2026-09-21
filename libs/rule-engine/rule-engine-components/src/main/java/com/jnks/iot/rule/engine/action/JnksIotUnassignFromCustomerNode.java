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
        name = "取消客户分配",
        configClazz = JnksIotUnassignFromCustomerNodeConfiguration.class,
        nodeDescription = "取消消息来源方实体与客户的分配关系",
        nodeDetails = "若消息来源方未分配给任何客户，规则节点将不做任何操作。<br><br>" +
                "若传入消息来源方是仪表板，将尝试根据配置中指定的标题查找客户。 " +
                "若客户不存在，将抛出异常。否则将仪表板从查找到的客户中解除分配。<br><br>" +
                "其他实体只能分配给一个客户，因此若来源方不是仪表板，配置中指定的客户标题将被忽略。",
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
