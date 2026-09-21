package com.jnks.iot.rule.engine.filter;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;

import java.util.Collections;
import java.util.List;

@Data
public class JnksIotCheckMessageNodeConfiguration implements NodeConfiguration<JnksIotCheckMessageNodeConfiguration>  {

    private List<String> messageNames;
    private List<String> metadataNames;

    private boolean checkAllKeys;


    @Override
    public JnksIotCheckMessageNodeConfiguration defaultConfiguration() {
        JnksIotCheckMessageNodeConfiguration configuration = new JnksIotCheckMessageNodeConfiguration();
        configuration.setMessageNames(Collections.emptyList());
        configuration.setMetadataNames(Collections.emptyList());
        configuration.setCheckAllKeys(true);
        return configuration;
    }
}
