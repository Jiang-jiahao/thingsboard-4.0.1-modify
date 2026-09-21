package com.jnks.iot.rule.engine.transform;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;

import java.util.Map;

@Data
public class JnksIotRenameKeysNodeConfiguration implements NodeConfiguration<JnksIotRenameKeysNodeConfiguration> {

    private JnksIotMsgSource renameIn;
    private Map<String, String> renameKeysMapping;

    @Override
    public JnksIotRenameKeysNodeConfiguration defaultConfiguration() {
        JnksIotRenameKeysNodeConfiguration configuration = new JnksIotRenameKeysNodeConfiguration();
        configuration.setRenameKeysMapping(Map.of("temperatureCelsius", "temperature"));
        configuration.setRenameIn(JnksIotMsgSource.DATA);
        return configuration;
    }

}
