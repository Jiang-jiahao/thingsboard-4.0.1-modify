package com.jnks.iot.server.actors.ruleChain;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.TbActorRef;
import com.jnks.iot.server.actors.TbEntityActorId;
import com.jnks.iot.server.actors.TbEntityTypeActorIdPredicate;
import com.jnks.iot.server.actors.service.ContextAwareActor;
import com.jnks.iot.server.actors.service.DefaultActorService;
import com.jnks.iot.server.actors.shared.RuleChainErrorActor;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageDataIterable;
import com.jnks.iot.server.common.data.rule.RuleChain;
import com.jnks.iot.server.common.data.rule.RuleChainType;
import com.jnks.iot.server.common.msg.TbActorMsg;
import com.jnks.iot.server.common.msg.queue.RuleEngineException;
import com.jnks.iot.server.dao.rule.RuleChainService;

import java.util.function.Function;

/**
 * Created by ashvayka on 15.03.18.
 */
@Slf4j
public abstract class RuleChainManagerActor extends ContextAwareActor {

    protected final TenantId tenantId;
    private final RuleChainService ruleChainService;
    @Getter
    protected RuleChain rootChain;
    @Getter
    protected TbActorRef rootChainActor;

    protected boolean ruleChainsInitialized;

    public RuleChainManagerActor(ActorSystemContext systemContext, TenantId tenantId) {
        super(systemContext);
        this.tenantId = tenantId;
        this.ruleChainService = systemContext.getRuleChainService();
    }

    protected void initRuleChains() {
        log.debug("[{}] Initializing rule chains", tenantId);
        for (RuleChain ruleChain : new PageDataIterable<>(link -> ruleChainService.findTenantRuleChainsByType(tenantId, RuleChainType.CORE, link), ContextAwareActor.ENTITY_PACK_LIMIT)) {
            RuleChainId ruleChainId = ruleChain.getId();
            log.debug("[{}|{}] Creating rule chain actor", ruleChainId.getEntityType(), ruleChain.getId());
            // 如果规则链不存在，则创建规则链actor
            TbActorRef actorRef = getOrCreateActor(ruleChainId, id -> ruleChain);
            visit(ruleChain, actorRef);
            log.debug("[{}|{}] Rule Chain actor created.", ruleChainId.getEntityType(), ruleChainId.getId());
        }
        ruleChainsInitialized = true;
    }

    protected void destroyRuleChains() {
        log.debug("[{}] Destroying rule chains", tenantId);
        for (RuleChain ruleChain : new PageDataIterable<>(link -> ruleChainService.findTenantRuleChainsByType(tenantId, RuleChainType.CORE, link), ContextAwareActor.ENTITY_PACK_LIMIT)) {
            ctx.stop(new TbEntityActorId(ruleChain.getId()));
        }
        ruleChainsInitialized = false;
    }

    protected void visit(RuleChain entity, TbActorRef actorRef) {
        // 判断该规则链是否是根规则链，并且是core类型，则赋值rootChain、rootChainActor
        if (entity != null && entity.isRoot() && entity.getType().equals(RuleChainType.CORE)) {
            rootChain = entity;
            rootChainActor = actorRef;
        }
    }

    protected TbActorRef getOrCreateActor(RuleChainId ruleChainId) {
        return getOrCreateActor(ruleChainId, eId -> ruleChainService.findRuleChainById(TenantId.SYS_TENANT_ID, eId));
    }

    protected TbActorRef getOrCreateActor(RuleChainId ruleChainId, Function<RuleChainId, RuleChain> provider) {
        return ctx.getOrCreateChildActor(new TbEntityActorId(ruleChainId),
                () -> DefaultActorService.RULE_DISPATCHER_NAME,
                () -> {
                    RuleChain ruleChain = provider.apply(ruleChainId);
                    if (ruleChain == null) {
                        return new RuleChainErrorActor.ActorCreator(systemContext, tenantId, ruleChainId,
                                new RuleEngineException("Rule Chain with id: " + ruleChainId + " not found!"));
                    } else {
                        return new RuleChainActor.ActorCreator(systemContext, tenantId, ruleChain);
                    }
                },
                () -> true);
    }

    protected TbActorRef getEntityActorRef(EntityId entityId) {
        TbActorRef target = null;
        if (entityId.getEntityType() == EntityType.RULE_CHAIN) {
            target = getOrCreateActor((RuleChainId) entityId);
        }
        return target;
    }

    protected void broadcast(TbActorMsg msg) {
        // 只发送规则链actor
        ctx.broadcastToChildren(msg, new TbEntityTypeActorIdPredicate(EntityType.RULE_CHAIN));
    }
}
