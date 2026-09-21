package com.jnks.iot.server.service.ws.telemetry.cmd.v2;

import lombok.Data;
import com.jnks.iot.server.service.ws.WsCmdType;

@Data
public class EntityDataUnsubscribeCmd implements UnsubscribeCmd {

    private final int cmdId;

    @Override
    public WsCmdType getType() {
        return WsCmdType.ENTITY_DATA_UNSUBSCRIBE;
    }
}
