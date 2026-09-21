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
import com.jnks.iot.server.common.data.relation.EntityRelation;
import com.jnks.iot.server.common.data.relation.EntitySearchDirection;
import com.jnks.iot.server.common.data.relation.RelationTypeGroup;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import static com.jnks.iot.common.util.DonAsynchron.withCallback;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "create relation",
        configClazz = JnksIotCreateRelationNodeConfiguration.class,
        nodeDescription = "Finds target entity specified in the configuration and creates a relation with the " +
                "incoming message originator based on the configured direction and type.",
        nodeDetails = "Useful when you need to create relations between entities dynamically depending on " +
                "incoming message payload, message originator type, name, etc.<br><br>" +
                "Target entity configuration: " +
                "<ul><li><strong>Device</strong> - use a device with the specified name as the target entity to create a relation with. " +
                "When selected, rule node allows us to use advanced mode to enable device creation if it doesn't exist. " +
                "In advanced mode, device profile name should be specified.</li>" +
                "<li><strong>Asset</strong> - use an asset with the specified name as the target entity to create a relation with. " +
                "When selected, rule node allows us to use advanced mode to enable device creation if it doesn't exist. " +
                "In advanced mode, asset profile name should be specified.</li>" +
                "<li><strong>Entity View</strong> - use entity view with the specified name as the target entity to create a relation with.</li>" +
                "<li><strong>Tenant</strong> - use current tenant as target entity to create a relation with.</li>" +
                "<li><strong>Customer</strong> - use customer with the specified title as the target entity to create a relation with. " +
                "When selected, rule node allows us to use advanced mode to enable customer creation if it doesn't exist.</li>" +
                "<li><strong>Dashboard</strong> - use a dashboard with the specified title as the target entity to create a relation with.</li>" +
                "<li><strong>User</strong> - use a user with the specified email as the target entity to create a relation with.</li></ul>" +
                "Advanced settings: " +
                "<ul><li><strong>Remove current relations</strong> - removes current relations with originator of the incoming message based on direction and type. " +
                "Useful in GPS tracking use cases where relation acts as a temporary indicator of a tracker presence in specific geofence.</li>" +
                "<li><strong>Change originator to target entity</strong> - useful when you need to process submitted message as a message from target entity.</li></ul>" +
                "Output connections: <code>Success</code> - if the relation already exists or successfully created, otherwise <code>Failure</code>.",
        configDirective = "jnksIotActionNodeCreateRelationConfig",
        icon = "add_circle",
        version = 1
)
public class JnksIotCreateRelationNode extends JnksIotAbstractRelationActionNode<JnksIotCreateRelationNodeConfiguration> {

    @Override
    protected JnksIotCreateRelationNodeConfiguration loadEntityNodeActionConfig(JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        var createRelationNodeConfiguration = JnksIotNodeUtils.convert(configuration, JnksIotCreateRelationNodeConfiguration.class);
        checkIfConfigEntityTypeIsSupported(createRelationNodeConfiguration.getEntityType());
        return createRelationNodeConfiguration;
    }

    @Override
    protected boolean createEntityIfNotExists() {
        return config.isCreateEntityIfNotExists();
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        var targetEntityIdFuture = getTargetEntityId(ctx, msg);
        var createRelationResultFuture = Futures.transformAsync(targetEntityIdFuture, targetEntityId -> {
            var originator = msg.getOriginator();
            var relationType = processPattern(msg, config.getRelationType());
            if (config.isRemoveCurrentRelations()) {
                var removalOfCurrentRelationsFuture = deleteRelationsByTypeAndDirection(ctx, msg, relationType, MoreExecutors.directExecutor());
                return Futures.transformAsync(removalOfCurrentRelationsFuture, __ ->
                        checkRelationAndCreateIfAbsent(ctx, originator, targetEntityId, relationType), MoreExecutors.directExecutor());
            }
            return checkRelationAndCreateIfAbsent(ctx, originator, targetEntityId, relationType);
        }, MoreExecutors.directExecutor());
        if (!config.isChangeOriginatorToRelatedEntity()) {
            withCallback(createRelationResultFuture,
                    relationCreated -> {
                        if (relationCreated) {
                            ctx.tellSuccess(msg);
                            return;
                        }
                        ctx.tellFailure(msg, new RuntimeException("Failed to create originator relation with target entity!"));
                    },
                    t -> ctx.tellFailure(msg, t), MoreExecutors.directExecutor());
            return;
        }
        withCallback(Futures.allAsList(targetEntityIdFuture, createRelationResultFuture), result -> {
            var targetEntityId = (EntityId) result.get(0);
            var relationCreated = (Boolean) result.get(1);
            if (relationCreated) {
                var transformedMsg = ctx.transformMsgOriginator(msg, targetEntityId);
                ctx.tellSuccess(transformedMsg);
                return;
            }
            ctx.tellFailure(msg, new RuntimeException("Failed to create originator relation with target entity!"));
        }, t -> ctx.tellFailure(msg, t), MoreExecutors.directExecutor());
    }

    private ListenableFuture<Boolean> checkRelationAndCreateIfAbsent(JnksIotContext ctx, EntityId originator, EntityId targetEntityId, String relationType) {
        EntityId fromId;
        EntityId toId;
        if (EntitySearchDirection.FROM.equals(config.getDirection())) {
            fromId = originator;
            toId = targetEntityId;
        } else {
            toId = originator;
            fromId = targetEntityId;
        }
        var checkRelationFuture = ctx.getRelationService().checkRelationAsync(ctx.getTenantId(), fromId, toId, relationType, RelationTypeGroup.COMMON);
        return Futures.transformAsync(checkRelationFuture, relationExists ->
                        relationExists ?
                                Futures.immediateFuture(true) :
                                ctx.getRelationService().
                                        saveRelationAsync(ctx.getTenantId(), new EntityRelation(fromId, toId, relationType, RelationTypeGroup.COMMON)),
                MoreExecutors.directExecutor());
    }

}
