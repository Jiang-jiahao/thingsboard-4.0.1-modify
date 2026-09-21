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
        name = "实体类型过滤",
        configClazz = JnksIotOriginatorTypeFilterNodeConfiguration.class,
        relationTypes = {JnksIotNodeConnectionType.TRUE, JnksIotNodeConnectionType.FALSE},
        nodeDescription = "按消息来源方实体的类型过滤传入消息",
        nodeDetails = "检查传入消息来源方的实体类型是否与过滤器中指定的某个值匹配。<br><br>" +
                "输出连接：<code>True</code>、<code>False</code>、<code>Failure</code>",
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
