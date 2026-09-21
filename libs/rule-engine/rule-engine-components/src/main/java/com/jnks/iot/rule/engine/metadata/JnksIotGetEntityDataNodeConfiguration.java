package com.jnks.iot.rule.engine.metadata;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;

import java.util.HashMap;

@Data
@EqualsAndHashCode(callSuper = true)
public class JnksIotGetEntityDataNodeConfiguration extends JnksIotGetMappedDataNodeConfiguration implements NodeConfiguration<JnksIotGetEntityDataNodeConfiguration> {

    private DataToFetch dataToFetch;

    @Override
    public JnksIotGetEntityDataNodeConfiguration defaultConfiguration() {
        var configuration = new JnksIotGetEntityDataNodeConfiguration();
        var dataMapping = new HashMap<String, String>();
        dataMapping.putIfAbsent("alarmThreshold", "threshold");
        configuration.setDataMapping(dataMapping);
        configuration.setDataToFetch(DataToFetch.ATTRIBUTES);
        configuration.setFetchTo(JnksIotMsgSource.METADATA);
        return configuration;
    }

}
