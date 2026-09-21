package com.jnks.iot.rule.engine.filter;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

/**
 * Created by ashvayka on 19.01.18.
 */
@Slf4j
@RuleNode(
        type = ComponentType.FILTER,
        name = "消息类型过滤",
        configClazz = JnksIotMsgTypeFilterNodeConfiguration.class,
        relationTypes = {JnksIotNodeConnectionType.TRUE, JnksIotNodeConnectionType.FALSE},
        nodeDescription = "按消息类型过滤传入消息",
        nodeDetails = "如果传入消息类型符合预期，则通过 <b>True</b> 链发送消息，否则使用 <b>False</b> 链。<br><br>" +
                "输出连接：<code>True</code>、<code>False</code>、<code>Failure</code>",
        configDirective = "jnksIotFilterNodeMessageTypeConfig")
public class JnksIotMsgTypeFilterNode implements JnksIotNode {

    JnksIotMsgTypeFilterNodeConfiguration config;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotMsgTypeFilterNodeConfiguration.class);
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        ctx.tellNext(msg, config.getMessageTypes().contains(msg.getType()) ? JnksIotNodeConnectionType.TRUE : JnksIotNodeConnectionType.FALSE);
    }

}
