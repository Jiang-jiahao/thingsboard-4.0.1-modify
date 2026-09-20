package com.jnks.iot.server.common.data.device.profile;

import com.jnks.iot.server.common.data.query.DynamicValue;

public class AnyTimeSchedule implements AlarmSchedule {

    @Override
    public AlarmScheduleType getType() {
        return AlarmScheduleType.ANY_TIME;
    }

    @Override
    public DynamicValue<String> getDynamicValue() {
        return null;
    }

}
