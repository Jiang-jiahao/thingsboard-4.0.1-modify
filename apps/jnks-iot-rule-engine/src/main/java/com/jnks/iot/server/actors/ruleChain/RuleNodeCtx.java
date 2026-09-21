package com.jnks.iot.server.actors.ruleChain;

import lombok.Data;
import com.jnks.iot.server.actors.JnksIotActorCtx;
import com.jnks.iot.server.actors.JnksIotActorRef;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.rule.RuleNode;

/**
 * Created by ashvayka on 19.03.18.
 */
@Data
public final class RuleNodeCtx {
    private final TenantId tenantId;
    private final JnksIotActorRef chainActor;
    private final JnksIotActorRef selfActor;
    private RuleNode self;

    RuleNodeCtx(TenantId tenantId, JnksIotActorCtx selfActor, RuleNode self) {
        this(tenantId, selfActor.getParentRef(), selfActor, self);
    }

    RuleNodeCtx(TenantId tenantId, JnksIotActorRef chainActor, JnksIotActorRef selfActor, RuleNode self) {
        this.tenantId = tenantId;
        this.chainActor = chainActor;
        this.selfActor = selfActor;
        this.self = self;
    }

}
