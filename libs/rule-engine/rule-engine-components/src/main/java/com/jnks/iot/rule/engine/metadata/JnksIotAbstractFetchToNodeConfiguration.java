package com.jnks.iot.rule.engine.metadata;

import lombok.Data;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;

@Data
public abstract class JnksIotAbstractFetchToNodeConfiguration {

    private JnksIotMsgSource fetchTo;

}
