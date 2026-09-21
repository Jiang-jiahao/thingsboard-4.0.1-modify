package com.jnks.iot.rule.engine.metadata;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;

import java.util.HashMap;

@Data
@EqualsAndHashCode(callSuper = true)
public class JnksIotGetOriginatorFieldsConfiguration extends JnksIotGetMappedDataNodeConfiguration implements NodeConfiguration<JnksIotGetOriginatorFieldsConfiguration> {

    private boolean ignoreNullStrings;

    @Override
    public JnksIotGetOriginatorFieldsConfiguration defaultConfiguration() {
        var configuration = new JnksIotGetOriginatorFieldsConfiguration();
        var dataMapping = new HashMap<String, String>();
        dataMapping.put("name", "originatorName");
        dataMapping.put("type", "originatorType");
        configuration.setDataMapping(dataMapping);
        configuration.setIgnoreNullStrings(false);
        configuration.setFetchTo(JnksIotMsgSource.METADATA);
        return configuration;
    }

}
