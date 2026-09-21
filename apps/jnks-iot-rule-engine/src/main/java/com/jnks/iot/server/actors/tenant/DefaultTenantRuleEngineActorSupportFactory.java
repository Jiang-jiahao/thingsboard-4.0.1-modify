package com.jnks.iot.server.actors.tenant;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.JnksIotActorCtx;
import com.jnks.iot.server.actors.JnksIotActorRef;
import com.jnks.iot.server.actors.JnksIotEntityActorId;
import com.jnks.iot.server.actors.JnksIotEntityTypeActorIdPredicate;
import com.jnks.iot.server.actors.JnksIotStringActorId;
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
import com.jnks.iot.server.common.msg.JnksIotActorMsg;
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
        private JnksIotActorRef rootChainActor;
        private boolean ruleChainsInitialized;

        private DefaultTenantRuleEngineActorSupport(ActorSystemContext systemContext, TenantId tenantId) {
            this.systemContext = systemContext;
            this.tenantId = tenantId;
            this.ruleChainService = systemContext.getRuleChainService();
        }

        @Override
        public JnksIotActorRef getOrCreateCalculatedFieldManagerActor(JnksIotActorCtx ctx) {
            return ctx.getOrCreateChildActor(new JnksIotStringActorId("CFM|" + tenantId),
                    () -> DefaultActorService.CF_MANAGER_DISPATCHER_NAME,
                    () -> new CalculatedFieldManagerActorCreator(systemContext, tenantId),
                    () -> true);
        }

        @Override
        public void initRuleChains(JnksIotActorCtx ctx) {
            log.debug("[{}] Initializing rule chains", tenantId);
            for (RuleChain ruleChain : new PageDataIterable<>(
                    link -> ruleChainService.findTenantRuleChainsByType(tenantId, RuleChainType.CORE, link),
                    ContextAwareActor.ENTITY_PACK_LIMIT)) {
                RuleChainId ruleChainId = ruleChain.getId();
                JnksIotActorRef actorRef = getOrCreateRuleChainActor(ctx, ruleChainId, id -> ruleChain);
                visit(ruleChain, actorRef);
            }
            ruleChainsInitialized = true;
        }

        @Override
        public void destroyRuleChains(JnksIotActorCtx ctx) {
            log.debug("[{}] Destroying rule chains", tenantId);
            for (RuleChain ruleChain : new PageDataIterable<>(
                    link -> ruleChainService.findTenantRuleChainsByType(tenantId, RuleChainType.CORE, link),
                    ContextAwareActor.ENTITY_PACK_LIMIT)) {
                ctx.stop(new JnksIotEntityActorId(ruleChain.getId()));
            }
            ruleChainsInitialized = false;
        }

        @Override
        public boolean isRuleChainsInitialized() {
            return ruleChainsInitialized;
        }

        @Override
        public JnksIotActorRef getRootChainActor() {
            return rootChainActor;
        }

        @Override
        public JnksIotActorRef getOrCreateRuleChainActor(JnksIotActorCtx ctx, RuleChainId ruleChainId) {
            return getOrCreateRuleChainActor(ctx, ruleChainId,
                    id -> ruleChainService.findRuleChainById(TenantId.SYS_TENANT_ID, id));
        }

        @Override
        public JnksIotActorRef getEntityActorRef(JnksIotActorCtx ctx, EntityId entityId) {
            if (entityId.getEntityType() == EntityType.RULE_CHAIN) {
                return getOrCreateRuleChainActor(ctx, (RuleChainId) entityId);
            }
            return null;
        }

        @Override
        public void visit(RuleChain ruleChain, JnksIotActorRef actorRef) {
            if (ruleChain != null && ruleChain.isRoot() && RuleChainType.CORE.equals(ruleChain.getType())) {
                rootChain = ruleChain;
                rootChainActor = actorRef;
            }
        }

        @Override
        public void broadcastToRuleChains(JnksIotActorCtx ctx, JnksIotActorMsg msg) {
            ctx.broadcastToChildren(msg, new JnksIotEntityTypeActorIdPredicate(EntityType.RULE_CHAIN));
        }

        private JnksIotActorRef getOrCreateRuleChainActor(JnksIotActorCtx ctx, RuleChainId ruleChainId, Function<RuleChainId, RuleChain> provider) {
            return ctx.getOrCreateChildActor(new JnksIotEntityActorId(ruleChainId),
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
