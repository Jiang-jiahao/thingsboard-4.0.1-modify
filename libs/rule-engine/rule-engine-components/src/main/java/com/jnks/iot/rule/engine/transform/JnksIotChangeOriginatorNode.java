package com.jnks.iot.rule.engine.transform;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.util.EntitiesAlarmOriginatorIdAsyncLoader;
import com.jnks.iot.rule.engine.util.EntitiesByNameAndTypeLoader;
import com.jnks.iot.rule.engine.util.EntitiesCustomerIdAsyncLoader;
import com.jnks.iot.rule.engine.util.EntitiesRelatedEntityIdAsyncLoader;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.List;
import java.util.NoSuchElementException;

import static com.jnks.iot.rule.engine.transform.OriginatorSource.ENTITY;
import static com.jnks.iot.rule.engine.transform.OriginatorSource.RELATED;

@Slf4j
@RuleNode(
        type = ComponentType.TRANSFORMATION,
        name = "更改来源方",
        configClazz = JnksIotChangeOriginatorNodeConfiguration.class,
        nodeDescription = "将消息来源方更改为租户/客户/关联实体/告警来源方/按名称模式的实体。",
        nodeDetails = "配置： <ul><li><strong>客户</strong> - 使用传入消息来源方的客户作为新的来源方。 " +
                "仅适用于分配给客户的、类型为以下之一的来源方：'User'、'Asset'、'Device'。</li>" +
                "<li><strong>租户</strong> - 使用当前租户作为新的来源方。</li>" +
                "<li><strong>关联实体</strong> - 使用关联实体作为新的来源方。基于配置的关系查询进行查找。 " +
                "如果找到多个关联实体，仅第一个实体用作新的来源方，其他实体会被丢弃。</li>" +
                "<li><strong>告警来源方</strong> - 使用告警来源方作为新的来源方。仅当传入消息来源方是告警实体时可用。</li>" +
                "<li><strong>按名称模式的实体</strong> - 指定新来源方的实体类型和名称模式。支持以下实体类型： " +
                "'Device'、'Asset'、'Entity View' 或 'User'。</li></ul>" +
                "输出连接：<code>Success</code>、<code>Failure</code>。",
        configDirective = "jnksIotTransformationNodeChangeOriginatorConfig",
        icon = "find_replace"
)
public class JnksIotChangeOriginatorNode extends JnksIotAbstractTransformNode<JnksIotChangeOriginatorNodeConfiguration> {

    @Override
    protected JnksIotChangeOriginatorNodeConfiguration loadNodeConfiguration(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        var config = JnksIotNodeUtils.convert(configuration, JnksIotChangeOriginatorNodeConfiguration.class);
        validateConfig(config);
        return config;
    }

    @Override
    protected ListenableFuture<List<JnksIotMsg>> transform(JnksIotContext ctx, JnksIotMsg msg) {
        ListenableFuture<? extends EntityId> newOriginatorFuture = getNewOriginator(ctx, msg);
        return Futures.transformAsync(newOriginatorFuture, newOriginator -> {
            if (newOriginator == null || newOriginator.isNullUid()) {
                return Futures.immediateFailedFuture(new NoSuchElementException("Failed to find new originator!"));
            }
            return Futures.immediateFuture(List.of(ctx.transformMsgOriginator(msg, newOriginator)));
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<? extends EntityId> getNewOriginator(JnksIotContext ctx, JnksIotMsg msg) {
        switch (config.getOriginatorSource()) {
            case CUSTOMER:
                return EntitiesCustomerIdAsyncLoader.findEntityIdAsync(ctx, msg.getOriginator());
            case TENANT:
                return Futures.immediateFuture(ctx.getTenantId());
            case RELATED:
                return EntitiesRelatedEntityIdAsyncLoader.findEntityAsync(ctx, msg.getOriginator(), config.getRelationsQuery());
            case ALARM_ORIGINATOR:
                return EntitiesAlarmOriginatorIdAsyncLoader.findEntityIdAsync(ctx, msg.getOriginator());
            case ENTITY:
                EntityType entityType = EntityType.valueOf(config.getEntityType());
                String entityName = JnksIotNodeUtils.processPattern(config.getEntityNamePattern(), msg);
                try {
                    EntityId targetEntity = EntitiesByNameAndTypeLoader.findEntityId(ctx, entityType, entityName);
                    return Futures.immediateFuture(targetEntity);
                } catch (IllegalStateException e) {
                    return Futures.immediateFailedFuture(e);
                }
            default:
                return Futures.immediateFailedFuture(new IllegalStateException("Unexpected originator source " + config.getOriginatorSource()));
        }
    }

    private void validateConfig(JnksIotChangeOriginatorNodeConfiguration conf) {
        if (conf.getOriginatorSource() == null) {
            log.debug("Originator source should be specified.");
            throw new IllegalArgumentException("Originator source should be specified.");
        }
        if (conf.getOriginatorSource().equals(RELATED) && conf.getRelationsQuery() == null) {
            log.debug("Relations query should be specified if 'Related entity' source is selected.");
            throw new IllegalArgumentException("Relations query should be specified if 'Related entity' source is selected.");
        }
        if (conf.getOriginatorSource().equals(ENTITY)) {
            if (conf.getEntityType() == null) {
                log.debug("Entity type should be specified if '{}' source is selected.", ENTITY);
                throw new IllegalArgumentException("Entity type should be specified if 'Entity by name pattern' source is selected.");
            }
            if (StringUtils.isEmpty(conf.getEntityNamePattern())) {
                log.debug("Name pattern should be specified if '{}' source is selected.", ENTITY);
                throw new IllegalArgumentException("Name pattern should be specified if 'Entity by name pattern' source is selected.");
            }
            EntitiesByNameAndTypeLoader.checkEntityType(EntityType.valueOf(conf.getEntityType()));
        }
    }

}
