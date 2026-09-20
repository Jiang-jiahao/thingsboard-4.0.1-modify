package com.jnks.iot.server.common.data.sync.ie;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import com.jnks.iot.server.common.data.rule.RuleChain;
import com.jnks.iot.server.common.data.rule.RuleChainMetaData;

@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Data
public class RuleChainExportData extends EntityExportData<RuleChain> {

    @JsonProperty(index = 3)
    @JsonIgnoreProperties({"ruleChainId", "version"})
    private RuleChainMetaData metaData;

}
