package com.jnks.iot.rule.engine.metadata;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;

import java.util.Collections;
import java.util.List;

/**
 * Created by ashvayka on 19.01.18.
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class JnksIotGetAttributesNodeConfiguration extends JnksIotAbstractFetchToNodeConfiguration implements NodeConfiguration<JnksIotGetAttributesNodeConfiguration> {

    private List<String> clientAttributeNames;
    private List<String> sharedAttributeNames;
    private List<String> serverAttributeNames;

    private List<String> latestTsKeyNames;

    private boolean tellFailureIfAbsent;
    private boolean getLatestValueWithTs;

    @Override
    public JnksIotGetAttributesNodeConfiguration defaultConfiguration() {
        var configuration = new JnksIotGetAttributesNodeConfiguration();
        configuration.setClientAttributeNames(Collections.emptyList());
        configuration.setSharedAttributeNames(Collections.emptyList());
        configuration.setServerAttributeNames(Collections.emptyList());
        configuration.setLatestTsKeyNames(Collections.emptyList());
        configuration.setTellFailureIfAbsent(true);
        configuration.setGetLatestValueWithTs(false);
        configuration.setFetchTo(JnksIotMsgSource.METADATA);
        return configuration;
    }

}
