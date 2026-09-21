package com.jnks.iot.server.common.msg.timeout;

import lombok.Data;
import com.jnks.iot.server.common.msg.JnksIotActorMsg;

/**
 * @author Andrew Shvayka
 */
@Data
public abstract class TimeoutMsg<T> implements JnksIotActorMsg {
    private final T id;
    private final long timeout;
}
