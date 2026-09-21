package com.jnks.iot.rule.engine.metadata;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.rule.engine.util.ContactBasedEntityDetails;

import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
public abstract class JnksIotAbstractGetEntityDetailsNodeConfiguration extends JnksIotAbstractFetchToNodeConfiguration {

    private List<ContactBasedEntityDetails> detailsList;

}
