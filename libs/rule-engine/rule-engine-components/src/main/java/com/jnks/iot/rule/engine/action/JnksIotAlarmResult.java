package com.jnks.iot.rule.engine.action;

import lombok.AllArgsConstructor;
import lombok.Data;
import com.jnks.iot.server.common.data.alarm.Alarm;
import com.jnks.iot.server.common.data.alarm.AlarmApiCallResult;

@Data
@AllArgsConstructor
public class JnksIotAlarmResult {
    boolean isCreated;
    boolean isUpdated;
    boolean isSeverityUpdated;
    boolean isCleared;
    Alarm alarm;

    public JnksIotAlarmResult(boolean isCreated, boolean isUpdated, boolean isCleared, Alarm alarm) {
        this.isCreated = isCreated;
        this.isUpdated = isUpdated;
        this.isCleared = isCleared;
        this.alarm = alarm;
    }

    public static JnksIotAlarmResult fromAlarmResult(AlarmApiCallResult result) {
        boolean isSeverityChanged = result.isSeverityChanged();
        return new JnksIotAlarmResult(
                result.isCreated(),
                result.isModified() && !isSeverityChanged,
                isSeverityChanged,
                result.isCleared(),
                result.getAlarm());
    }
}
