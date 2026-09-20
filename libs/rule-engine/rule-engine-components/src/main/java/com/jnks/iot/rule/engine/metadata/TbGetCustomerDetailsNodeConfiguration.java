package com.jnks.iot.rule.engine.metadata;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.rule.engine.util.TbMsgSource;

import java.util.Collections;

@Data
@EqualsAndHashCode(callSuper = true)
public class TbGetCustomerDetailsNodeConfiguration extends TbAbstractGetEntityDetailsNodeConfiguration implements NodeConfiguration<TbGetCustomerDetailsNodeConfiguration> {

    @Override
    public TbGetCustomerDetailsNodeConfiguration defaultConfiguration() {
        var configuration = new TbGetCustomerDetailsNodeConfiguration();
        configuration.setDetailsList(Collections.emptyList());
        configuration.setFetchTo(TbMsgSource.DATA);
        return configuration;
    }

}
