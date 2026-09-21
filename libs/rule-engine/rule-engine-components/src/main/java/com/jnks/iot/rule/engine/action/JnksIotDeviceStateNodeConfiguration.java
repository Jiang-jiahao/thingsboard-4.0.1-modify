package com.jnks.iot.rule.engine.action;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;

@Data
public class JnksIotDeviceStateNodeConfiguration implements NodeConfiguration<JnksIotDeviceStateNodeConfiguration> {

    private JnksIotMsgType event;

    @Override
    public JnksIotDeviceStateNodeConfiguration defaultConfiguration() {
        var config = new JnksIotDeviceStateNodeConfiguration();
        config.setEvent(JnksIotMsgType.ACTIVITY_EVENT);
        return config;
    }

}
