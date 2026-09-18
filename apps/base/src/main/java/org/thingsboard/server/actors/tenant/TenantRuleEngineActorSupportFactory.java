package org.thingsboard.server.actors.tenant;

import org.thingsboard.server.actors.ActorSystemContext;
import org.thingsboard.server.common.data.id.TenantId;

/**
 * Creates tenant scoped rule-engine actor support implementation.
 */
public interface TenantRuleEngineActorSupportFactory {

    TenantRuleEngineActorSupport create(ActorSystemContext systemContext, TenantId tenantId);

}
