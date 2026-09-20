package com.jnks.iot.rule.engine.transform;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.rule.engine.util.TbMsgSource;

import java.util.Map;

@Data
public class TbRenameKeysNodeConfiguration implements NodeConfiguration<TbRenameKeysNodeConfiguration> {

    private TbMsgSource renameIn;
    private Map<String, String> renameKeysMapping;

    @Override
    public TbRenameKeysNodeConfiguration defaultConfiguration() {
        TbRenameKeysNodeConfiguration configuration = new TbRenameKeysNodeConfiguration();
        configuration.setRenameKeysMapping(Map.of("temperatureCelsius", "temperature"));
        configuration.setRenameIn(TbMsgSource.DATA);
        return configuration;
    }

}
