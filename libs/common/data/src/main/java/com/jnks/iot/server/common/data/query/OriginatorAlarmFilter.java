package com.jnks.iot.server.common.data.query;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.alarm.AlarmSeverity;
import com.jnks.iot.server.common.data.id.EntityId;

import java.util.List;

@NoArgsConstructor
@AllArgsConstructor
@Getter
public class OriginatorAlarmFilter {
    private EntityId originatorId;
    private List<String> typeList;
    private List<AlarmSeverity> severityList;
}
