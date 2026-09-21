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
        name = "删除关系",
        configClazz = JnksIotDeleteRelationNodeConfiguration.class,
        nodeDescription = "根据配置的方向和类型，删除与传入消息来源方的关系。",
        nodeDetails = "当需要根据传入消息的负载、 " +
                "消息来源方类型、名称等动态删除实体之间的关系时，该节点很有用。<br><br>" +
                "若启用 <strong>删除与特定实体的关系</strong>，需指定要删除关系的目标实体。 " +
                "否则，规则节点将根据配置的方向和类型，删除与消息来源方的所有关系。<br><br>" +
                "目标实体配置： " +
                "<ul><li><strong>设备</strong> - 使用指定名称的设备作为删除关系的目标实体。</li>" +
                "<li><strong>资产</strong> - 使用指定名称的资产作为删除关系的目标实体。</li>" +
                "<li><strong>实体视图</strong> - 使用指定名称的实体视图作为删除关系的目标实体。</li>" +
                "<li><strong>租户</strong> - 使用当前租户作为删除关系的目标实体。</li>" +
                "<li><strong>客户</strong> - 使用指定标题的客户作为删除关系的目标实体。</li>" +
                "<li><strong>仪表板</strong> - 使用指定标题的仪表板作为删除关系的目标实体。</li>" +
                "<li><strong>用户</strong> - 使用指定邮箱的用户作为删除关系的目标实体。</li></ul>" +
                "输出连接：<code>Success</code> - 关系删除成功时；否则 <code>Failure</code>。",
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
