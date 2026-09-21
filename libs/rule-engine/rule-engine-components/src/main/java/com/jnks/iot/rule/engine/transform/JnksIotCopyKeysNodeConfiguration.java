package com.jnks.iot.rule.engine.transform;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;

import java.util.Collections;
import java.util.Set;

@Data
public class JnksIotCopyKeysNodeConfiguration implements NodeConfiguration<JnksIotCopyKeysNodeConfiguration> {

    private JnksIotMsgSource copyFrom;
    private Set<String> keys;

    @Override
    public JnksIotCopyKeysNodeConfiguration defaultConfiguration() {
        JnksIotCopyKeysNodeConfiguration configuration = new JnksIotCopyKeysNodeConfiguration();
        configuration.setKeys(Collections.emptySet());
        configuration.setCopyFrom(JnksIotMsgSource.DATA);
        return configuration;
    }

}
