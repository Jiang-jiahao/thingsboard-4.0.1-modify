package org.thingsboard.server.service.ws.telemetry.cmd.v2;

import lombok.Data;
import org.thingsboard.server.service.ws.WsCmdType;

@Data
public class AlarmCountUnsubscribeCmd implements UnsubscribeCmd {

    private final int cmdId;

    @Override
    public WsCmdType getType() {
        return WsCmdType.ALARM_COUNT_UNSUBSCRIBE;
    }
}
