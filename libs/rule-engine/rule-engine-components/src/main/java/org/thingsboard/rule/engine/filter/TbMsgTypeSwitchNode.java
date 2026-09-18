package org.thingsboard.rule.engine.filter;

import lombok.extern.slf4j.Slf4j;
import org.thingsboard.rule.engine.api.EmptyNodeConfiguration;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;

@Slf4j
@RuleNode(
        type = ComponentType.FILTER,
        name = "message type switch",
        configClazz = EmptyNodeConfiguration.class,
        relationTypes = {}, // should always be empty. We add the relation types for this node in AnnotationComponentDiscoveryService.
        nodeDescription = "Route incoming messages by Message Type",
        nodeDetails = "Sends messages with message types <b>\"Post attributes\", \"Post telemetry\", \"RPC Request\"</b>" +
                " etc. via corresponding chain, otherwise <b>Other</b> chain is used.<br><br>" +
                "Output connections: <i>Message type connection</i>, <code>Other</code> - if message type is custom or <code>Failure</code>",
        configDirective = "tbNodeEmptyConfig")
public class TbMsgTypeSwitchNode implements TbNode {

    EmptyNodeConfiguration config;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) {
        ctx.tellNext(msg, msg.getInternalType().getRuleNodeConnection());
    }

}
