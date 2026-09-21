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
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.JnksIotPair;

@Slf4j
@RuleNode(
        type = ComponentType.ENRICHMENT,
        name = "租户属性",
        configClazz = JnksIotGetEntityDataNodeConfiguration.class,
        version = 1,
        nodeDescription = "将消息来源方所属租户的属性或最新遥测添加到消息或消息元数据中",
        nodeDetails = "当需要获取某些公共配置或阈值时很有用， " +
                "这些公共配置或阈值以租户属性或遥测数据的形式存储，可用于后续的消息处理。<br><br>" +
                "输出连接：<code>Success</code>、<code>Failure</code>。",
        configDirective = "jnksIotEnrichmentNodeTenantAttributesConfig")
public class JnksIotGetTenantAttributeNode extends JnksIotAbstractGetEntityDataNode<TenantId> {

    @Override
    public JnksIotGetEntityDataNodeConfiguration loadNodeConfiguration(JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        var config = JnksIotNodeUtils.convert(configuration, JnksIotGetEntityDataNodeConfiguration.class);
        checkIfMappingIsNotEmptyOrElseThrow(config.getDataMapping());
        checkDataToFetchSupportedOrElseThrow(config.getDataToFetch());
        return config;
    }

    @Override
    public ListenableFuture<TenantId> findEntityAsync(JnksIotContext ctx, EntityId originator) {
        return Futures.immediateFuture(ctx.getTenantId());
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        return fromVersion == 0 ? upgradeToUseFetchToAndDataToFetch(oldConfiguration) : new JnksIotPair<>(false, oldConfiguration);
    }

}
