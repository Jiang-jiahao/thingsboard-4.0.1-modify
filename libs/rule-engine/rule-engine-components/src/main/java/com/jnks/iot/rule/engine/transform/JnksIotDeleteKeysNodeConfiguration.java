package com.jnks.iot.rule.engine.transform;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;

import java.util.Collections;
import java.util.Set;

@Data
public class JnksIotDeleteKeysNodeConfiguration implements NodeConfiguration<JnksIotDeleteKeysNodeConfiguration> {

    private JnksIotMsgSource deleteFrom;
    private Set<String> keys;

    @Override
    public JnksIotDeleteKeysNodeConfiguration defaultConfiguration() {
        JnksIotDeleteKeysNodeConfiguration configuration = new JnksIotDeleteKeysNodeConfiguration();
        configuration.setKeys(Collections.emptySet());
        configuration.setDeleteFrom(JnksIotMsgSource.DATA);
        return configuration;
    }

}
