package com.jnks.iot.server.common.msg.plugin;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.plugin.ComponentLifecycleEvent;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.aware.TenantAwareMsg;
import com.jnks.iot.server.common.msg.cluster.ToAllNodesMsg;

import java.io.Serial;
import java.util.Optional;

/**
 * @author Andrew Shvayka
 */
@Data
public class ComponentLifecycleMsg implements TenantAwareMsg, ToAllNodesMsg {

    @Serial
    private static final long serialVersionUID = -5303421482781273062L;

    private final TenantId tenantId;
    private final EntityId entityId;
    private final ComponentLifecycleEvent event;
    private final String oldName;
    private final String name;
    private final EntityId oldProfileId;
    private final EntityId profileId;

    public ComponentLifecycleMsg(TenantId tenantId, EntityId entityId, ComponentLifecycleEvent event) {
        this(tenantId, entityId, event, null, null, null, null);
    }

    @Builder
    private ComponentLifecycleMsg(TenantId tenantId, EntityId entityId, ComponentLifecycleEvent event, String oldName, String name, EntityId oldProfileId, EntityId profileId) {
        this.tenantId = tenantId;
        this.entityId = entityId;
        this.event = event;
        this.oldName = oldName;
        this.name = name;
        this.oldProfileId = oldProfileId;
        this.profileId = profileId;
    }

    public Optional<RuleChainId> getRuleChainId() {
        return entityId.getEntityType() == EntityType.RULE_CHAIN ? Optional.of((RuleChainId) entityId) : Optional.empty();
    }

    @Override
    public MsgType getMsgType() {
        return MsgType.COMPONENT_LIFE_CYCLE_MSG;
    }

}
