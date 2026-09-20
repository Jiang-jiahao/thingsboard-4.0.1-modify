package com.jnks.iot.rule.engine.metadata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.rule.engine.util.TbMsgSource;

@Data
@EqualsAndHashCode(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TbFetchDeviceCredentialsNodeConfiguration extends TbAbstractFetchToNodeConfiguration implements NodeConfiguration<TbFetchDeviceCredentialsNodeConfiguration> {

    @Override
    public TbFetchDeviceCredentialsNodeConfiguration defaultConfiguration() {
        var configuration = new TbFetchDeviceCredentialsNodeConfiguration();
        configuration.setFetchTo(TbMsgSource.METADATA);
        return configuration;
    }

}
