package com.jnks.iot.server.common.msg.plugin;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.plugin.ComponentLifecycleEvent;
import com.jnks.iot.server.common.msg.MsgType;

/**
 * @author Andrew Shvayka
 * This class used only to tell local rule-node actor like 'existing.getSelfActor().tellWithHighPriority(new RuleNodeUpdatedMs( ...'
 * Never serialized to/from proto, otherwise you need to change proto mappers in ProtoUtils class
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class RuleNodeUpdatedMsg extends ComponentLifecycleMsg {

    public RuleNodeUpdatedMsg(TenantId tenantId, EntityId entityId) {
        super(tenantId, entityId, ComponentLifecycleEvent.UPDATED);
    }

    @Override
    public MsgType getMsgType() {
        return MsgType.RULE_NODE_UPDATED_MSG;
    }
}