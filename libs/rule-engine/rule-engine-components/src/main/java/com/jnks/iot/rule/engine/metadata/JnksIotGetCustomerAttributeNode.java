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
import com.jnks.iot.rule.engine.util.EntitiesCustomerIdAsyncLoader;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.JnksIotPair;

@Slf4j
@RuleNode(
        type = ComponentType.ENRICHMENT,
        name = "客户属性",
        configClazz = JnksIotGetEntityDataNodeConfiguration.class,
        version = 1,
        nodeDescription = "将消息来源方所属客户的属性或最新遥测添加到消息或消息元数据中",
        nodeDetails = "适用于多客户解决方案：每个客户都有不同的配置或阈值， " +
                "这些配置或阈值以客户属性或遥测数据的形式存储，用于动态的消息过滤、转换， " +
                "或在超过阈值时执行创建告警等动作。<br><br>" +
                "输出连接：<code>Success</code>、<code>Failure</code>。",
        configDirective = "jnksIotEnrichmentNodeCustomerAttributesConfig")
public class JnksIotGetCustomerAttributeNode extends JnksIotAbstractGetEntityDataNode<CustomerId> {

    private static final String CUSTOMER_NOT_FOUND_MESSAGE = "Failed to find customer for entity with id: %s and type: %s";

    @Override
    protected JnksIotGetEntityDataNodeConfiguration loadNodeConfiguration(JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        var config = JnksIotNodeUtils.convert(configuration, JnksIotGetEntityDataNodeConfiguration.class);
        checkIfMappingIsNotEmptyOrElseThrow(config.getDataMapping());
        checkDataToFetchSupportedOrElseThrow(config.getDataToFetch());
        return config;
    }

    @Override
    protected ListenableFuture<CustomerId> findEntityAsync(JnksIotContext ctx, EntityId originator) {
        return Futures.transformAsync(EntitiesCustomerIdAsyncLoader.findEntityIdAsync(ctx, originator),
                checkIfEntityIsPresentOrThrow(String.format(CUSTOMER_NOT_FOUND_MESSAGE, originator.getId(), originator.getEntityType().getNormalName())),
                ctx.getDbCallbackExecutor()
        );
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        return fromVersion == 0 ? upgradeToUseFetchToAndDataToFetch(oldConfiguration) : new JnksIotPair<>(false, oldConfiguration);
    }

}
