package com.jnks.iot.server.service.ws.telemetry;

import lombok.Data;
import com.jnks.iot.server.service.ws.WebSocketSessionRef;

/**
 * Created by ashvayka on 27.03.18.
 */
@Data
public class TelemetryWebSocketTextMsg {

    private final WebSocketSessionRef sessionRef;
    private final String payload;

}
