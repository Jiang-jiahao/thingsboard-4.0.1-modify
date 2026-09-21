package com.jnks.iot.server.actors.device;

import lombok.Data;
import com.jnks.iot.server.common.msg.rpc.ToDeviceRpcRequestActorMsg;

/**
 * @author Andrew Shvayka
 */
@Data
public class ToDeviceRpcRequestMetadata {
    private final ToDeviceRpcRequestActorMsg msg;
    private final boolean sent;
    private int retries;
    private boolean delivered;
}
