package org.thingsboard.rule.engine.metadata;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
public abstract class TbGetMappedDataNodeConfiguration extends TbAbstractFetchToNodeConfiguration {

    private Map<String, String> dataMapping;

}
