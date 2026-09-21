package com.jnks.iot.server.actors.ruleChain;

import lombok.Data;
import com.jnks.iot.server.common.data.id.EntityId;

/**
 * Created by ashvayka on 19.03.18.
 */

@Data
final class RuleNodeRelation {

    private final EntityId in;
    private final EntityId out;
    private final String type;

}
