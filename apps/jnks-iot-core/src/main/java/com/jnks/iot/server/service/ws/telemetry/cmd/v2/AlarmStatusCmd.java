package com.jnks.iot.server.service.ws.telemetry.cmd.v2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.alarm.AlarmSeverity;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.service.ws.WsCmd;
import com.jnks.iot.server.service.ws.WsCmdType;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AlarmStatusCmd implements WsCmd {

    private int cmdId;
    private EntityId originatorId;
    private List<String> typeList;
    private List<AlarmSeverity> severityList;

    @Override
    public WsCmdType getType() {
        return WsCmdType.ALARM_STATUS;
    }
}
