package com.jnks.iot.server.actors.shared;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.TbActor;
import com.jnks.iot.server.actors.TbActorId;
import com.jnks.iot.server.actors.TbEntityActorId;
import com.jnks.iot.server.actors.service.ContextAwareActor;
import com.jnks.iot.server.actors.service.ContextBasedCreator;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.TbActorMsg;
import com.jnks.iot.server.common.msg.aware.RuleChainAwareMsg;
import com.jnks.iot.server.common.msg.queue.RuleEngineException;

@Slf4j
public class RuleChainErrorActor extends ContextAwareActor {

    private final TenantId tenantId;
    private final RuleEngineException error;

    private RuleChainErrorActor(ActorSystemContext systemContext, TenantId tenantId, RuleEngineException error) {
        super(systemContext);
        this.tenantId = tenantId;
        this.error = error;
    }

    @Override
    protected boolean doProcess(TbActorMsg msg) {
        if (msg instanceof RuleChainAwareMsg rcMsg) {
            log.debug("[{}] Reply with {} for message {}", tenantId, error.getMessage(), msg);
            rcMsg.getMsg().getCallback().onFailure(error);
            return true;
        } else {
            return false;
        }
    }

    public static class ActorCreator extends ContextBasedCreator {

        private final TenantId tenantId;
        private final RuleChainId ruleChainId;
        private final RuleEngineException error;

        public ActorCreator(ActorSystemContext context, TenantId tenantId, RuleChainId ruleChainId, RuleEngineException error) {
            super(context);
            this.tenantId = tenantId;
            this.ruleChainId = ruleChainId;
            this.error = error;
        }

        @Override
        public TbActorId createActorId() {
            return new TbEntityActorId(ruleChainId);
        }

        @Override
        public TbActor createActor() {
            return new RuleChainErrorActor(context, tenantId, error);
        }
    }

}
