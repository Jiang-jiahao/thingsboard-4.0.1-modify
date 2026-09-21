package com.jnks.iot.rule.engine.telemetry;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.server.common.data.DataConstants;

import java.util.Collections;
import java.util.List;

@Data
public class JnksIotMsgDeleteAttributesNodeConfiguration implements NodeConfiguration<JnksIotMsgDeleteAttributesNodeConfiguration> {

    private String scope;
    private List<String> keys;
    private boolean sendAttributesDeletedNotification;
    private boolean notifyDevice;

    @Override
    public JnksIotMsgDeleteAttributesNodeConfiguration defaultConfiguration() {
        JnksIotMsgDeleteAttributesNodeConfiguration configuration = new JnksIotMsgDeleteAttributesNodeConfiguration();
        configuration.setScope(DataConstants.SERVER_SCOPE);
        configuration.setKeys(Collections.emptyList());
        configuration.setSendAttributesDeletedNotification(false);
        configuration.setNotifyDevice(false);
        return configuration;
    }
}
