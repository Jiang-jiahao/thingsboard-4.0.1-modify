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
        name = "创建关系",
        configClazz = JnksIotCreateRelationNodeConfiguration.class,
        nodeDescription = "查找配置中指定的目标实体，并根据配置的方向和类型与 " +
                "传入消息的来源方创建关系。",
        nodeDetails = "当你需要根据传入消息负载、 " +
                "消息来源方类型、名称等动态创建实体间关系时非常有用。<br><br>" +
                "目标实体配置： " +
                "<ul><li><strong>设备</strong> - 使用指定名称的设备作为目标实体来创建关系。 " +
                "选中后，规则节点允许使用高级模式，在设备不存在时自动创建设备。 " +
                "在高级模式下，需要指定设备档案名称。</li>" +
                "<li><strong>资产</strong> - 使用指定名称的资产作为目标实体来创建关系。 " +
                "选中后，规则节点允许使用高级模式，在设备不存在时自动创建设备。 " +
                "在高级模式下，需要指定资产档案名称。</li>" +
                "<li><strong>实体视图</strong> - 使用指定名称的实体视图作为目标实体来创建关系。</li>" +
                "<li><strong>租户</strong> - 使用当前租户作为目标实体来创建关系。</li>" +
                "<li><strong>客户</strong> - 使用指定标题的客户作为目标实体来创建关系。 " +
                "选中后，规则节点允许使用高级模式，在客户不存在时自动创建客户。</li>" +
                "<li><strong>仪表板</strong> - 使用指定标题的仪表板作为目标实体来创建关系。</li>" +
                "<li><strong>用户</strong> - 使用指定邮箱的用户作为目标实体来创建关系。</li></ul>" +
                "高级设置： " +
                "<ul><li><strong>移除当前关系</strong> - 根据方向和类型移除与传入消息来源方的当前关系。 " +
                "适用于 GPS 追踪场景，此时关系可作为追踪器出现在特定地理围栏中的临时标识。</li>" +
                "<li><strong>将来源方更改为目标实体</strong> - 当你需要将提交的消息作为来自目标实体的消息处理时非常有用。</li></ul>" +
                "输出连接：<code>Success</code> - 若关系已存在或创建成功，否则为 <code>Failure</code>。",
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
