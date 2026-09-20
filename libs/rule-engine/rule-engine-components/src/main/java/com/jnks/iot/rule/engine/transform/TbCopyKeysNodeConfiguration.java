package com.jnks.iot.rule.engine.transform;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.rule.engine.util.TbMsgSource;

import java.util.Collections;
import java.util.Set;

@Data
public class TbCopyKeysNodeConfiguration implements NodeConfiguration<TbCopyKeysNodeConfiguration> {

    private TbMsgSource copyFrom;
    private Set<String> keys;

    @Override
    public TbCopyKeysNodeConfiguration defaultConfiguration() {
        TbCopyKeysNodeConfiguration configuration = new TbCopyKeysNodeConfiguration();
        configuration.setKeys(Collections.emptySet());
        configuration.setCopyFrom(TbMsgSource.DATA);
        return configuration;
    }

}
