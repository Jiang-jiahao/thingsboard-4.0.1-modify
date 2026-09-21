package com.jnks.iot.server.common.msg;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@ToString
@EqualsAndHashCode
public abstract class JnksIotRuleEngineActorMsg implements JnksIotActorMsg {

    @Getter
    protected final JnksIotMsg msg;

    public JnksIotRuleEngineActorMsg(JnksIotMsg msg) {
        this.msg = msg;
    }
}
