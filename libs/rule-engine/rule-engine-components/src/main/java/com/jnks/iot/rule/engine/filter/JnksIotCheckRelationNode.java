package com.jnks.iot.rule.engine.filter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.EntityIdFactory;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.relation.EntityRelation;
import com.jnks.iot.server.common.data.relation.EntitySearchDirection;
import com.jnks.iot.server.common.data.relation.RelationTypeGroup;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.List;

import static com.jnks.iot.common.util.DonAsynchron.withCallback;

/**
 * Created by ashvayka on 19.01.18.
 */
@Slf4j
@RuleNode(
        type = ComponentType.FILTER,
        name = "check relation presence",
        configClazz = JnksIotCheckRelationNodeConfiguration.class,
        version = 1,
        relationTypes = {JnksIotNodeConnectionType.TRUE, JnksIotNodeConnectionType.FALSE},
        nodeDescription = "Checks the presence of the relation between the originator of the message and other entities.",
        nodeDetails = "If 'check relation to specific entity' is selected, you should specify a related entity. " +
                "Otherwise, the rule node checks the presence of a relation to any entity. " +
                "In both cases, relation lookup is based on configured direction and type.<br><br>" +
                "Output connections: <code>True</code>, <code>False</code>, <code>Failure</code>",
        configDirective = "jnksIotFilterNodeCheckRelationConfig")
public class JnksIotCheckRelationNode implements JnksIotNode {

    private JnksIotCheckRelationNodeConfiguration config;
    private EntityId singleEntityId;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotCheckRelationNodeConfiguration.class);
        if (config.isCheckForSingleEntity()) {
            if (StringUtils.isEmpty(config.getEntityType()) || StringUtils.isEmpty(config.getEntityId())) {
                throw new JnksIotNodeException("Entity should be specified!");
            }
            this.singleEntityId = EntityIdFactory.getByTypeAndId(config.getEntityType(), config.getEntityId());
            ctx.checkTenantEntity(singleEntityId);
        }
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) throws JnksIotNodeException {
        ListenableFuture<Boolean> checkRelationFuture = config.isCheckForSingleEntity() ?
                processSingle(ctx, msg) : processList(ctx, msg);
        withCallback(checkRelationFuture,
                filterResult -> ctx.tellNext(msg, filterResult ? JnksIotNodeConnectionType.TRUE : JnksIotNodeConnectionType.FALSE),
                t -> ctx.tellFailure(msg, t), ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<Boolean> processSingle(JnksIotContext ctx, JnksIotMsg msg) {
        EntityId from;
        EntityId to;
        if (EntitySearchDirection.FROM.name().equals(config.getDirection())) {
            to = singleEntityId;
            from = msg.getOriginator();
        } else {
            from = singleEntityId;
            to = msg.getOriginator();
        }
        return ctx.getRelationService().checkRelationAsync(ctx.getTenantId(), from, to, config.getRelationType(), RelationTypeGroup.COMMON);
    }

    private ListenableFuture<Boolean> processList(JnksIotContext ctx, JnksIotMsg msg) {
        ListenableFuture<List<EntityRelation>> relationListFuture = EntitySearchDirection.FROM.name().equals(config.getDirection()) ?
                ctx.getRelationService().findByFromAndTypeAsync(ctx.getTenantId(), msg.getOriginator(), config.getRelationType(), RelationTypeGroup.COMMON) :
                ctx.getRelationService().findByToAndTypeAsync(ctx.getTenantId(), msg.getOriginator(), config.getRelationType(), RelationTypeGroup.COMMON);
        return Futures.transformAsync(relationListFuture, this::isEmptyList, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<Boolean> isEmptyList(List<EntityRelation> entityRelations) {
        return entityRelations.isEmpty() ? Futures.immediateFuture(false) : Futures.immediateFuture(true);
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        if (fromVersion == 0) {
            var newConfigObjectNode = (ObjectNode) oldConfiguration;
            var directionPropertyName = "direction";
            if (!newConfigObjectNode.has(directionPropertyName)) {
                throw new JnksIotNodeException("property to update: '" + directionPropertyName + "' doesn't exists in configuration!");
            }
            String direction = newConfigObjectNode.get(directionPropertyName).asText();
            if (EntitySearchDirection.TO.name().equals(direction)) {
                newConfigObjectNode.put(directionPropertyName, EntitySearchDirection.FROM.name());
                return new JnksIotPair<>(true, newConfigObjectNode);
            }
            if (EntitySearchDirection.FROM.name().equals(direction)) {
                newConfigObjectNode.put(directionPropertyName, EntitySearchDirection.TO.name());
                return new JnksIotPair<>(true, newConfigObjectNode);
            }
            throw new JnksIotNodeException("property to update: '" + directionPropertyName + "' has invalid value!");
        }
        return new JnksIotPair<>(false, oldConfiguration);
    }

}
