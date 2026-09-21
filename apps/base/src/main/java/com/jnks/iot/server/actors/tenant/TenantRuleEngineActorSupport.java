package com.jnks.iot.server.actors.tenant;

import com.jnks.iot.server.actors.JnksIotActorCtx;
import com.jnks.iot.server.actors.JnksIotActorRef;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.rule.RuleChain;
import com.jnks.iot.server.common.msg.JnksIotActorMsg;

/**
 * Encapsulates rule-engine specific actor logic for tenant actor.
 */
public interface TenantRuleEngineActorSupport {

    JnksIotActorRef getOrCreateCalculatedFieldManagerActor(JnksIotActorCtx ctx);

    void initRuleChains(JnksIotActorCtx ctx);

    void destroyRuleChains(JnksIotActorCtx ctx);

    boolean isRuleChainsInitialized();

    JnksIotActorRef getRootChainActor();

    JnksIotActorRef getOrCreateRuleChainActor(JnksIotActorCtx ctx, RuleChainId ruleChainId);

    JnksIotActorRef getEntityActorRef(JnksIotActorCtx ctx, EntityId entityId);

    void visit(RuleChain ruleChain, JnksIotActorRef actorRef);

    void broadcastToRuleChains(JnksIotActorCtx ctx, JnksIotActorMsg msg);

}
