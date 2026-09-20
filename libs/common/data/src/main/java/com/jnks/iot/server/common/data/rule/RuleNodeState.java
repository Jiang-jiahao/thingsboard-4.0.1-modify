package com.jnks.iot.server.common.data.rule;

import lombok.Data;
import com.jnks.iot.server.common.data.BaseData;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.RuleNodeId;
import com.jnks.iot.server.common.data.id.RuleNodeStateId;

@Data
public class RuleNodeState extends BaseData<RuleNodeStateId> {

    private RuleNodeId ruleNodeId;
    private EntityId entityId;
    private String stateData;

    public RuleNodeState() {
        super();
    }

    public RuleNodeState(RuleNodeStateId id) {
        super(id);
    }

    public RuleNodeState(RuleNodeState event) {
        super(event);
    }
}
