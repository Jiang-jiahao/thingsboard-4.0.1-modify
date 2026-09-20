package com.jnks.iot.server.actors.tenant;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.TbActorCtx;
import com.jnks.iot.server.actors.TbActorRef;
import com.jnks.iot.server.actors.TbEntityActorId;
import com.jnks.iot.server.actors.TbEntityTypeActorIdPredicate;
import com.jnks.iot.server.actors.TbStringActorId;
import com.jnks.iot.server.actors.calculatedField.CalculatedFieldManagerActorCreator;
import com.jnks.iot.server.actors.ruleChain.RuleChainActor;
import com.jnks.iot.server.actors.shared.RuleChainErrorActor;
import com.jnks.iot.server.actors.service.ContextAwareActor;
import com.jnks.iot.server.actors.service.DefaultActorService;
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

@Component
public class DefaultTenantRuleEngineActorSupportFactory implements TenantRuleEngineActorSupportFactory {

    @Override
    public TenantRuleEngineActorSupport create(ActorSystemContext systemContext, TenantId tenantId) {
        return new DefaultTenantRuleEngineActorSupport(systemContext, tenantId);
    }

    @Slf4j
    private static class DefaultTenantRuleEngineActorSupport implements TenantRuleEngineActorSupport {

        private final ActorSystemContext systemContext;
        private final TenantId tenantId;
        private final RuleChainService ruleChainService;

        private RuleChain rootChain;
        private TbActorRef rootChainActor;
        private boolean ruleChainsInitialized;

        private DefaultTenantRuleEngineActorSupport(ActorSystemContext systemContext, TenantId tenantId) {
            this.systemContext = systemContext;
            this.tenantId = tenantId;
            this.ruleChainService = systemContext.getRuleChainService();
        }

        @Override
        public TbActorRef getOrCreateCalculatedFieldManagerActor(TbActorCtx ctx) {
            return ctx.getOrCreateChildActor(new TbStringActorId("CFM|" + tenantId),
                    () -> DefaultActorService.CF_MANAGER_DISPATCHER_NAME,
                    () -> new CalculatedFieldManagerActorCreator(systemContext, tenantId),
                    () -> true);
        }

        @Override
        public void initRuleChains(TbActorCtx ctx) {
            log.debug("[{}] Initializing rule chains", tenantId);
            for (RuleChain ruleChain : new PageDataIterable<>(
                    link -> ruleChainService.findTenantRuleChainsByType(tenantId, RuleChainType.CORE, link),
                    ContextAwareActor.ENTITY_PACK_LIMIT)) {
                RuleChainId ruleChainId = ruleChain.getId();
                TbActorRef actorRef = getOrCreateRuleChainActor(ctx, ruleChainId, id -> ruleChain);
                visit(ruleChain, actorRef);
            }
            ruleChainsInitialized = true;
        }

        @Override
        public void destroyRuleChains(TbActorCtx ctx) {
            log.debug("[{}] Destroying rule chains", tenantId);
            for (RuleChain ruleChain : new PageDataIterable<>(
                    link -> ruleChainService.findTenantRuleChainsByType(tenantId, RuleChainType.CORE, link),
                    ContextAwareActor.ENTITY_PACK_LIMIT)) {
                ctx.stop(new TbEntityActorId(ruleChain.getId()));
            }
            ruleChainsInitialized = false;
        }

        @Override
        public boolean isRuleChainsInitialized() {
            return ruleChainsInitialized;
        }

        @Override
        public TbActorRef getRootChainActor() {
            return rootChainActor;
        }

        @Override
        public TbActorRef getOrCreateRuleChainActor(TbActorCtx ctx, RuleChainId ruleChainId) {
            return getOrCreateRuleChainActor(ctx, ruleChainId,
                    id -> ruleChainService.findRuleChainById(TenantId.SYS_TENANT_ID, id));
        }

        @Override
        public TbActorRef getEntityActorRef(TbActorCtx ctx, EntityId entityId) {
            if (entityId.getEntityType() == EntityType.RULE_CHAIN) {
                return getOrCreateRuleChainActor(ctx, (RuleChainId) entityId);
            }
            return null;
        }

        @Override
        public void visit(RuleChain ruleChain, TbActorRef actorRef) {
            if (ruleChain != null && ruleChain.isRoot() && RuleChainType.CORE.equals(ruleChain.getType())) {
                rootChain = ruleChain;
                rootChainActor = actorRef;
            }
        }

        @Override
        public void broadcastToRuleChains(TbActorCtx ctx, TbActorMsg msg) {
            ctx.broadcastToChildren(msg, new TbEntityTypeActorIdPredicate(EntityType.RULE_CHAIN));
        }

        private TbActorRef getOrCreateRuleChainActor(TbActorCtx ctx, RuleChainId ruleChainId, Function<RuleChainId, RuleChain> provider) {
            return ctx.getOrCreateChildActor(new TbEntityActorId(ruleChainId),
                    () -> DefaultActorService.RULE_DISPATCHER_NAME,
                    () -> {
                        RuleChain ruleChain = provider.apply(ruleChainId);
                        if (ruleChain == null) {
                            return new RuleChainErrorActor.ActorCreator(systemContext, tenantId, ruleChainId,
                                    new RuleEngineException("Rule Chain with id: " + ruleChainId + " not found!"));
                        }
                        return new RuleChainActor.ActorCreator(systemContext, tenantId, ruleChain);
                    },
                    () -> true);
        }
    }
}
