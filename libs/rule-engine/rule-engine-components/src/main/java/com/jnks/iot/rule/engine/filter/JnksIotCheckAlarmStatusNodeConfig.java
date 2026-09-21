package com.jnks.iot.rule.engine.filter;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.server.common.data.alarm.AlarmStatus;

import java.util.Arrays;
import java.util.List;

@Data
public class JnksIotCheckAlarmStatusNodeConfig implements NodeConfiguration<JnksIotCheckAlarmStatusNodeConfig> {

    private List<AlarmStatus> alarmStatusList;

    @Override
    public JnksIotCheckAlarmStatusNodeConfig defaultConfiguration() {
        var config = new JnksIotCheckAlarmStatusNodeConfig();
        config.setAlarmStatusList(Arrays.asList(AlarmStatus.ACTIVE_ACK, AlarmStatus.ACTIVE_UNACK));
        return config;
    }

}
