package com.jnks.iot.rule.engine.transform;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;

@Data
public class JnksIotJsonPathNodeConfiguration implements NodeConfiguration<JnksIotJsonPathNodeConfiguration> {

    static final String DEFAULT_JSON_PATH = "$";
    private String jsonPath;

    @Override
    public JnksIotJsonPathNodeConfiguration defaultConfiguration() {
        JnksIotJsonPathNodeConfiguration configuration = new JnksIotJsonPathNodeConfiguration();
        configuration.setJsonPath(DEFAULT_JSON_PATH);
        return configuration;
    }

}
