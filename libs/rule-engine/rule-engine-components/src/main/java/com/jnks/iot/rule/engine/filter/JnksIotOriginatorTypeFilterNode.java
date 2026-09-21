package com.jnks.iot.rule.engine.filter;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

@Slf4j
@RuleNode(
        type = ComponentType.FILTER,
        name = "entity type filter",
        configClazz = JnksIotOriginatorTypeFilterNodeConfiguration.class,
        relationTypes = {JnksIotNodeConnectionType.TRUE, JnksIotNodeConnectionType.FALSE},
        nodeDescription = "Filter incoming messages by the type of message originator entity",
        nodeDetails = "Checks that the entity type of the incoming message originator matches one of the values specified in the filter.<br><br>" +
                "Output connections: <code>True</code>, <code>False</code>, <code>Failure</code>",
        configDirective = "jnksIotFilterNodeOriginatorTypeConfig")
public class JnksIotOriginatorTypeFilterNode implements JnksIotNode {

    JnksIotOriginatorTypeFilterNodeConfiguration config;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotOriginatorTypeFilterNodeConfiguration.class);
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        EntityType originatorType = msg.getOriginator().getEntityType();
        ctx.tellNext(msg, config.getOriginatorTypes().contains(originatorType) ? JnksIotNodeConnectionType.TRUE : JnksIotNodeConnectionType.FALSE);
    }

}
