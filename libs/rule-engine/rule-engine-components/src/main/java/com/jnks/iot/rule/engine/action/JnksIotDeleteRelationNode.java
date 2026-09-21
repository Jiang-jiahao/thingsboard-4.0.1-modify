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
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.relation.EntitySearchDirection;
import com.jnks.iot.server.common.data.relation.RelationTypeGroup;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import static com.jnks.iot.common.util.DonAsynchron.withCallback;


@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "delete relation",
        configClazz = JnksIotDeleteRelationNodeConfiguration.class,
        nodeDescription = "Deletes relation with the incoming message originator based on the configured direction and type.",
        nodeDetails = "Useful when you need to remove relations between entities dynamically depending on incoming message payload, " +
                "message originator type, name, etc.<br><br>" +
                "If <strong>Delete relation with specific entity</strong> enabled, target entity to delete relation with should be specified. " +
                "Otherwise, rule node will delete all relations with the message originator based on the configured direction and type.<br><br>" +
                "Target entity configuration: " +
                "<ul><li><strong>Device</strong> - use a device with the specified name as the target entity to delete relation with.</li>" +
                "<li><strong>Asset</strong> - use an asset with the specified name as the target entity to delete relation with.</li>" +
                "<li><strong>Entity View</strong> - use entity view with the specified name as the target entity to delete relation with.</li>" +
                "<li><strong>Tenant</strong> - use current tenant as target entity to delete relation with.</li>" +
                "<li><strong>Customer</strong> - use customer with the specified title as the target entity to delete relation with.</li>" +
                "<li><strong>Dashboard</strong> - use a dashboard with the specified title as the target entity to delete relation with.</li>" +
                "<li><strong>User</strong> - use a user with the specified email as the target entity to delete relation with.</li></ul>" +
                "Output connections: <code>Success</code> - If the relation(s) successfully deleted, otherwise <code>Failure</code>.",
        configDirective = "jnksIotActionNodeDeleteRelationConfig",
        icon = "remove_circle",
        version = 1
)
public class JnksIotDeleteRelationNode extends JnksIotAbstractRelationActionNode<JnksIotDeleteRelationNodeConfiguration> {

    @Override
    protected JnksIotDeleteRelationNodeConfiguration loadEntityNodeActionConfig(JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        var deleteRelationNodeConfiguration = JnksIotNodeUtils.convert(configuration, JnksIotDeleteRelationNodeConfiguration.class);
        if (!deleteRelationNodeConfiguration.isDeleteForSingleEntity()) {
            return deleteRelationNodeConfiguration;
        }
        checkIfConfigEntityTypeIsSupported(deleteRelationNodeConfiguration.getEntityType());
        return deleteRelationNodeConfiguration;
    }

    @Override
    protected boolean createEntityIfNotExists() {
        return false;
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        ListenableFuture<Boolean> deleteResultFuture = config.isDeleteForSingleEntity() ?
                Futures.transformAsync(getTargetEntityId(ctx, msg), targetEntityId ->
                        deleteRelationToSpecificEntity(ctx, msg, targetEntityId), MoreExecutors.directExecutor()) :
                deleteRelationsByTypeAndDirection(ctx, msg, ctx.getDbCallbackExecutor());
        withCallback(deleteResultFuture, deleted -> {
                    if (deleted) {
                        ctx.tellSuccess(msg);
                        return;
                    }
                    ctx.tellFailure(msg, new RuntimeException("Failed to delete relation(s) with originator!"));
                },
                t -> ctx.tellFailure(msg, t), MoreExecutors.directExecutor());
    }

    private ListenableFuture<Boolean> deleteRelationToSpecificEntity(JnksIotContext ctx, JnksIotMsg msg, EntityId targetEntityId) {
        EntityId fromId;
        EntityId toId;
        if (EntitySearchDirection.FROM.equals(config.getDirection())) {
            fromId = msg.getOriginator();
            toId = targetEntityId;
        } else {
            toId = msg.getOriginator();
            fromId = targetEntityId;
        }
        var relationType = processPattern(msg, config.getRelationType());
        var tenantId = ctx.getTenantId();
        var relationService = ctx.getRelationService();
        return Futures.transformAsync(relationService.checkRelationAsync(tenantId, fromId, toId, relationType, RelationTypeGroup.COMMON),
                relationExists -> {
                    if (relationExists) {
                        return relationService.deleteRelationAsync(tenantId, fromId, toId, relationType, RelationTypeGroup.COMMON);
                    }
                    return Futures.immediateFuture(true);
                }, MoreExecutors.directExecutor());
    }

}
