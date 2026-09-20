package com.jnks.iot.rule.engine.profile.state;

import lombok.Data;
import com.jnks.iot.server.common.data.alarm.AlarmSeverity;

import java.util.Map;

@Data
public class PersistedAlarmState {

    private Map<AlarmSeverity, PersistedAlarmRuleState> createRuleStates;
    private PersistedAlarmRuleState clearRuleState;

}
