package com.jnks.iot.server.actors.device;

import lombok.Data;
import com.jnks.iot.server.gen.transport.TransportProtos.SessionType;

/**
 * @author Andrew Shvayka
 */
@Data
public class SessionInfo {
    private final SessionType type;
    private final String nodeId;
}
