package com.jnks.iot.server.actors.tenant;

import com.jnks.iot.server.actors.TbActorCtx;
import com.jnks.iot.server.actors.TbActorRef;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.rule.RuleChain;
import com.jnks.iot.server.common.msg.TbActorMsg;

/**
 * Encapsulates rule-engine specific actor logic for tenant actor.
 */
public interface TenantRuleEngineActorSupport {

    TbActorRef getOrCreateCalculatedFieldManagerActor(TbActorCtx ctx);

    void initRuleChains(TbActorCtx ctx);

    void destroyRuleChains(TbActorCtx ctx);

    boolean isRuleChainsInitialized();

    TbActorRef getRootChainActor();

    TbActorRef getOrCreateRuleChainActor(TbActorCtx ctx, RuleChainId ruleChainId);

    TbActorRef getEntityActorRef(TbActorCtx ctx, EntityId entityId);

    void visit(RuleChain ruleChain, TbActorRef actorRef);

    void broadcastToRuleChains(TbActorCtx ctx, TbActorMsg msg);

}
