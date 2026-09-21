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
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;

/**
 * Created by ashvayka on 19.01.18.
 */
@Slf4j
@RuleNode(type = ComponentType.ENRICHMENT,
        name = "来源方属性",
        configClazz = JnksIotGetAttributesNodeConfiguration.class,
        version = 1,
        nodeDescription = "将消息来源方的属性和/或最新时序数据添加到消息或消息元数据中",
        nodeDetails = "当你需要从消息来源方检索 " +
                "那些未包含在传入消息中的某些属性或最新遥测读数，以便将其用于后续消息处理时非常有用。 " +
                "例如根据属性中存储的阈值过滤消息。<br><br>" +
                "输出连接：<code>Success</code>、<code>Failure</code>。",
        configDirective = "jnksIotEnrichmentNodeOriginatorAttributesConfig")
public class JnksIotGetAttributesNode extends JnksIotAbstractGetAttributesNode<JnksIotGetAttributesNodeConfiguration, EntityId> {

    @Override
    protected JnksIotGetAttributesNodeConfiguration loadNodeConfiguration(JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        return JnksIotNodeUtils.convert(configuration, JnksIotGetAttributesNodeConfiguration.class);
    }

    @Override
    protected ListenableFuture<EntityId> findEntityIdAsync(JnksIotContext ctx, JnksIotMsg msg) {
        return Futures.immediateFuture(msg.getOriginator());
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        return fromVersion == 0 ?
                upgradeRuleNodesWithOldPropertyToUseFetchTo(
                        oldConfiguration,
                        "fetchToData",
                        JnksIotMsgSource.DATA.name(),
                        JnksIotMsgSource.METADATA.name()) :
                new JnksIotPair<>(false, oldConfiguration);
    }

}
