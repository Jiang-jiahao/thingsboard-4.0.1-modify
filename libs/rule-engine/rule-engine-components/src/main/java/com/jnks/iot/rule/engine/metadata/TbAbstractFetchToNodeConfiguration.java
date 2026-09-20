package com.jnks.iot.rule.engine.metadata;

import lombok.Data;
import com.jnks.iot.rule.engine.util.TbMsgSource;

@Data
public abstract class TbAbstractFetchToNodeConfiguration {

    private TbMsgSource fetchTo;

}
