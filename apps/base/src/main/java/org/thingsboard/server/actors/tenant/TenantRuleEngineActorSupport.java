package org.thingsboard.server.actors.tenant;

import org.thingsboard.server.actors.TbActorCtx;
import org.thingsboard.server.actors.TbActorRef;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.RuleChainId;
import org.thingsboard.server.common.data.rule.RuleChain;
import org.thingsboard.server.common.msg.TbActorMsg;

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
