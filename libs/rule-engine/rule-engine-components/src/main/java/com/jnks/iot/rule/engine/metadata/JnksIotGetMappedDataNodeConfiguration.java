package com.jnks.iot.rule.engine.metadata;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
public abstract class JnksIotGetMappedDataNodeConfiguration extends JnksIotAbstractFetchToNodeConfiguration {

    private Map<String, String> dataMapping;

}
