package com.jnks.iot.rule.engine.filter;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.EmptyNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.msg.JnksIotMsg;

@Slf4j
public abstract class JnksIotAbstractTypeSwitchNode implements JnksIotNode {

    private EmptyNodeConfiguration config;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) throws JnksIotNodeException {
        ctx.tellNext(msg, getRelationType(ctx, msg.getOriginator()));
    }

    protected abstract String getRelationType(JnksIotContext ctx, EntityId originator) throws JnksIotNodeException;

}
