package com.jnks.iot.rule.engine.action;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.id.AssetId;
import com.jnks.iot.server.common.data.id.DashboardId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityViewId;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "分配给客户",
        configClazz = JnksIotAssignToCustomerNodeConfiguration.class,
        nodeDescription = "将消息来源方实体分配给客户",
        nodeDetails = "根据标题查找目标客户，并将消息来源方实体分配给该客户。 " +
                "若启用「客户不存在时创建新客户」，规则节点会在客户不存在时创建新客户。",
        configDirective = "jnksIotActionNodeAssignToCustomerConfig",
        icon = "add_circle",
        version = 1
)
public class JnksIotAssignToCustomerNode extends JnksIotAbstractCustomerActionNode<JnksIotAssignToCustomerNodeConfiguration> {

    @Override
    protected boolean createCustomerIfNotExists() {
        return config.isCreateCustomerIfNotExists();
    }

    @Override
    protected JnksIotAssignToCustomerNodeConfiguration loadCustomerNodeActionConfig(JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        return JnksIotNodeUtils.convert(configuration, JnksIotAssignToCustomerNodeConfiguration.class);
    }

    @Override
    protected ListenableFuture<Void> processCustomerAction(JnksIotContext ctx, JnksIotMsg msg) {
        var customerIdFuture = getCustomerIdFuture(ctx, msg);
        return Futures.transform(customerIdFuture, customerId -> {
            var originator = msg.getOriginator();
            switch (originator.getEntityType()) {
                case ASSET ->
                        ctx.getAssetService().assignAssetToCustomer(ctx.getTenantId(), new AssetId(originator.getId()), customerId);
                case DEVICE ->
                        ctx.getDeviceService().assignDeviceToCustomer(ctx.getTenantId(), new DeviceId(originator.getId()), customerId);
                case ENTITY_VIEW ->
                        ctx.getEntityViewService().assignEntityViewToCustomer(ctx.getTenantId(), new EntityViewId(originator.getId()), customerId);
                case DASHBOARD ->
                        ctx.getDashboardService().assignDashboardToCustomer(ctx.getTenantId(), new DashboardId(originator.getId()), customerId);
            }
            return null;
        }, MoreExecutors.directExecutor());
    }

}
