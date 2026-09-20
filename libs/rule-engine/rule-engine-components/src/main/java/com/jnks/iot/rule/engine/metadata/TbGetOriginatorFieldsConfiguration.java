package com.jnks.iot.rule.engine.metadata;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.rule.engine.util.TbMsgSource;

import java.util.HashMap;

@Data
@EqualsAndHashCode(callSuper = true)
public class TbGetOriginatorFieldsConfiguration extends TbGetMappedDataNodeConfiguration implements NodeConfiguration<TbGetOriginatorFieldsConfiguration> {

    private boolean ignoreNullStrings;

    @Override
    public TbGetOriginatorFieldsConfiguration defaultConfiguration() {
        var configuration = new TbGetOriginatorFieldsConfiguration();
        var dataMapping = new HashMap<String, String>();
        dataMapping.put("name", "originatorName");
        dataMapping.put("type", "originatorType");
        configuration.setDataMapping(dataMapping);
        configuration.setIgnoreNullStrings(false);
        configuration.setFetchTo(TbMsgSource.METADATA);
        return configuration;
    }

}
