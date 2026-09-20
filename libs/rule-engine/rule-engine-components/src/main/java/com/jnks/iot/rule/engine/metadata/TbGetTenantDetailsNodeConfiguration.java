package com.jnks.iot.rule.engine.metadata;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.rule.engine.util.TbMsgSource;

import java.util.Collections;

@Data
@EqualsAndHashCode(callSuper = true)
public class TbGetTenantDetailsNodeConfiguration extends TbAbstractGetEntityDetailsNodeConfiguration implements NodeConfiguration<TbGetTenantDetailsNodeConfiguration> {

    @Override
    public TbGetTenantDetailsNodeConfiguration defaultConfiguration() {
        var configuration = new TbGetTenantDetailsNodeConfiguration();
        configuration.setDetailsList(Collections.emptyList());
        configuration.setFetchTo(TbMsgSource.DATA);
        return configuration;
    }

}
