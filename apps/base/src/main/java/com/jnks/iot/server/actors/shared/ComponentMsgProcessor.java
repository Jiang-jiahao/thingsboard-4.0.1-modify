package com.jnks.iot.server.actors.shared;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.JnksIotActorCtx;
import com.jnks.iot.server.actors.stats.StatsPersistTick;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.plugin.ComponentLifecycleState;
import com.jnks.iot.server.common.data.tenant.profile.TenantProfileConfiguration;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.queue.PartitionChangeMsg;
import com.jnks.iot.server.common.msg.queue.RuleNodeException;

import java.util.concurrent.ScheduledFuture;

@Slf4j
public abstract class ComponentMsgProcessor<T extends EntityId> extends AbstractContextAwareMsgProcessor {

    protected final TenantId tenantId;
    protected final T entityId;
    protected ComponentLifecycleState state;

    protected ComponentMsgProcessor(ActorSystemContext systemContext, TenantId tenantId, T id) {
        super(systemContext);
        this.tenantId = tenantId;
        this.entityId = id;
    }

    protected TenantProfileConfiguration getTenantProfileConfiguration() {
        return systemContext.getTenantProfileCache().get(tenantId).getProfileData().getConfiguration();
    }

    public abstract String getComponentName();

    public abstract void start(JnksIotActorCtx context) throws Exception;

    public abstract void stop(JnksIotActorCtx context) throws Exception;

    public abstract void onPartitionChangeMsg(PartitionChangeMsg msg) throws Exception;

    public void onCreated(JnksIotActorCtx context) throws Exception {
        start(context);
    }

    public void onUpdate(JnksIotActorCtx context) throws Exception {
        restart(context);
    }

    public void onActivate(JnksIotActorCtx context) throws Exception {
        restart(context);
    }

    public void onSuspend(JnksIotActorCtx context) throws Exception {
        stop(context);
    }

    public void onStop(JnksIotActorCtx context) throws Exception {
        stop(context);
    }

    private void restart(JnksIotActorCtx context) throws Exception {
        stop(context);
        start(context);
    }

    public ScheduledFuture<?> scheduleStatsPersistTick(JnksIotActorCtx context, long statsPersistFrequency) {
        return schedulePeriodicMsgWithDelay(context, StatsPersistTick.INSTANCE, statsPersistFrequency, statsPersistFrequency);
    }

    protected boolean checkMsgValid(JnksIotMsg jnksIotMsg) {
        var valid = jnksIotMsg.isValid();
        if (!valid) {
            if (log.isTraceEnabled()) {
                log.trace("Skip processing of message: {} because it is no longer valid!", jnksIotMsg);
            }
        }
        return valid;
    }

    protected void checkComponentStateActive(JnksIotMsg jnksIotMsg) throws RuleNodeException {
        if (state != ComponentLifecycleState.ACTIVE) {
            log.debug("Component is not active. Current state [{}] for processor [{}][{}] tenant [{}]", state, entityId.getEntityType(), entityId, tenantId);
            RuleNodeException ruleNodeException = getInactiveException();
            if (jnksIotMsg != null) {
                jnksIotMsg.getCallback().onFailure(ruleNodeException);
            }
            throw ruleNodeException;
        }
    }

    abstract protected RuleNodeException getInactiveException();

}
