package com.jnks.iot.server.actors.tenant;

import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.common.data.id.TenantId;

/**
 * Creates tenant scoped rule-engine actor support implementation.
 */
public interface TenantRuleEngineActorSupportFactory {

    TenantRuleEngineActorSupport create(ActorSystemContext systemContext, TenantId tenantId);

}
