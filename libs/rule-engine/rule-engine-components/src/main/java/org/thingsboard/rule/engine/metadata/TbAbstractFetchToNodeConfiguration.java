package org.thingsboard.rule.engine.metadata;

import lombok.Data;
import org.thingsboard.rule.engine.util.TbMsgSource;

@Data
public abstract class TbAbstractFetchToNodeConfiguration {

    private TbMsgSource fetchTo;

}
