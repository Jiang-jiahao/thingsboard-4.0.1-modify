package com.jnks.iot.rule.engine.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.util.EntitiesRelatedEntityIdAsyncLoader;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.JnksIotPair;

import java.util.Arrays;

@Slf4j
@RuleNode(
        type = ComponentType.ENRICHMENT,
        name = "相关实体数据",
        configClazz = JnksIotGetRelatedDataNodeConfiguration.class,
        version = 1,
        nodeDescription = "将来源方相关实体的属性、最新遥测或字段添加到消息或消息元数据中",
        nodeDetails = "根据配置的关系查询查找相关实体。 " +
                "若找到多个相关实体，则仅使用第一个实体进行消息增强，其他实体将被丢弃。 " +
                "当你需要从与消息来源方存在关系的实体检索数据，并将其用于后续消息处理时非常有用。<br><br>" +
                "输出连接：<code>Success</code>、<code>Failure</code>。",
        configDirective = "jnksIotEnrichmentNodeRelatedAttributesConfig")
public class JnksIotGetRelatedAttributeNode extends JnksIotAbstractGetEntityDataNode<EntityId> {

    private static final String RELATED_ENTITY_NOT_FOUND_MESSAGE = "Failed to find related entity to message originator using relation query specified in the configuration!";

    @Override
    public JnksIotGetRelatedDataNodeConfiguration loadNodeConfiguration(JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        var config = JnksIotNodeUtils.convert(configuration, JnksIotGetRelatedDataNodeConfiguration.class);
        checkIfMappingIsNotEmptyOrElseThrow(config.getDataMapping());
        checkDataToFetchSupportedOrElseThrow(config.getDataToFetch());
        return config;
    }

    @Override
    public ListenableFuture<EntityId> findEntityAsync(JnksIotContext ctx, EntityId originator) {
        var relatedAttrConfig = (JnksIotGetRelatedDataNodeConfiguration) config;
        return Futures.transformAsync(
                EntitiesRelatedEntityIdAsyncLoader.findEntityAsync(ctx, originator, relatedAttrConfig.getRelationsQuery()),
                checkIfEntityIsPresentOrThrow(RELATED_ENTITY_NOT_FOUND_MESSAGE),
                ctx.getDbCallbackExecutor());
    }

    @Override
    protected void checkDataToFetchSupportedOrElseThrow(DataToFetch dataToFetch) throws JnksIotNodeException {
        if (dataToFetch == null) {
            throw new JnksIotNodeException("DataToFetch property cannot be null! Supported values are: " + Arrays.toString(DataToFetch.values()));
        }
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        return fromVersion == 0 ? upgradeToUseFetchToAndDataToFetch(oldConfiguration) : new JnksIotPair<>(false, oldConfiguration);
    }

}
