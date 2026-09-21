package com.jnks.iot.rule.engine.filter;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.EmptyNodeConfiguration;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.plugin.ComponentType;

@Slf4j
@RuleNode(
        type = ComponentType.FILTER,
        name = "实体类型分流",
        configClazz = EmptyNodeConfiguration.class,
        relationTypes = {}, // should always be empty. We add the relation types for this node in AnnotationComponentDiscoveryService.
        nodeDescription = "Route incoming messages by Message Originator Type",
        nodeDetails = "根据实体类型（'Device'、'Asset' 等）将消息路由到相应的链。<br><br>" +
                "输出连接：<i>消息来源方类型</i> 或 <code>Failure</code>",
        configDirective = "jnksIotNodeEmptyConfig")
public class JnksIotOriginatorTypeSwitchNode extends JnksIotAbstractTypeSwitchNode {

    @Override
    protected String getRelationType(JnksIotContext ctx, EntityId originator) {
        return originator.getEntityType().getNormalName();
    }

}
