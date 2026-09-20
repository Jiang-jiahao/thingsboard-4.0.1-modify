package com.jnks.iot.rule.engine.filter;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.TbContext;
import com.jnks.iot.rule.engine.api.TbNode;
import com.jnks.iot.rule.engine.api.TbNodeConfiguration;
import com.jnks.iot.rule.engine.api.TbNodeException;
import com.jnks.iot.rule.engine.api.util.TbNodeUtils;
import com.jnks.iot.server.common.data.msg.TbNodeConnectionType;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.TbMsg;

/**
 * Created by ashvayka on 19.01.18.
 */
@Slf4j
@RuleNode(
        type = ComponentType.FILTER,
        name = "message type filter",
        configClazz = TbMsgTypeFilterNodeConfiguration.class,
        relationTypes = {TbNodeConnectionType.TRUE, TbNodeConnectionType.FALSE},
        nodeDescription = "Filter incoming messages by Message Type",
        nodeDetails = "If incoming message type is expected - send Message via <b>True</b> chain, otherwise <b>False</b> chain is used.<br><br>" +
                "Output connections: <code>True</code>, <code>False</code>, <code>Failure</code>",
        configDirective = "tbFilterNodeMessageTypeConfig")
public class TbMsgTypeFilterNode implements TbNode {

    TbMsgTypeFilterNodeConfiguration config;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbMsgTypeFilterNodeConfiguration.class);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) {
        ctx.tellNext(msg, config.getMessageTypes().contains(msg.getType()) ? TbNodeConnectionType.TRUE : TbNodeConnectionType.FALSE);
    }

}
