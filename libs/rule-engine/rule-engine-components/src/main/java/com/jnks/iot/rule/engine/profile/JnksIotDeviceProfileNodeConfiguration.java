package com.jnks.iot.rule.engine.profile;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class JnksIotDeviceProfileNodeConfiguration implements NodeConfiguration<JnksIotDeviceProfileNodeConfiguration> {

    private boolean persistAlarmRulesState;
    private boolean fetchAlarmRulesStateOnStart;

    @Override
    public JnksIotDeviceProfileNodeConfiguration defaultConfiguration() {
        return new JnksIotDeviceProfileNodeConfiguration();
    }
}
