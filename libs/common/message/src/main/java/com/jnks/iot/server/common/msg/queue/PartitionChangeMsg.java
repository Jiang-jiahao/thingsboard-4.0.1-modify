package com.jnks.iot.server.common.msg.queue;

import lombok.Data;
import lombok.Getter;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.JnksIotActorMsg;

/**
 * @author Andrew Shvayka
 */
@Data
public final class PartitionChangeMsg implements JnksIotActorMsg {

    @Getter
    private final ServiceType serviceType;

    @Override
    public MsgType getMsgType() {
        return MsgType.PARTITION_CHANGE_MSG;
    }
}
