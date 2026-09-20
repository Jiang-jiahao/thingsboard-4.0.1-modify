package com.jnks.iot.rule.engine.action;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.server.common.data.msg.TbMsgType;

@Data
public class TbDeviceStateNodeConfiguration implements NodeConfiguration<TbDeviceStateNodeConfiguration> {

    private TbMsgType event;

    @Override
    public TbDeviceStateNodeConfiguration defaultConfiguration() {
        var config = new TbDeviceStateNodeConfiguration();
        config.setEvent(TbMsgType.ACTIVITY_EVENT);
        return config;
    }

}
