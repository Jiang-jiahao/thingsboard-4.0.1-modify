package com.jnks.iot.server.actors.ruleChain;

import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.JnksIotRuleNodeUpdateException;
import com.jnks.iot.server.actors.service.ComponentActor;
import com.jnks.iot.server.actors.shared.ComponentMsgProcessor;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.rule.trigger.RuleEngineComponentLifecycleEventTrigger;
import com.jnks.iot.server.common.data.plugin.ComponentLifecycleEvent;
import com.jnks.iot.server.common.msg.JnksIotActorStopReason;

public abstract class RuleEngineComponentActor<T extends EntityId, P extends ComponentMsgProcessor<T>> extends ComponentActor<T, P> {

    public RuleEngineComponentActor(ActorSystemContext systemContext, TenantId tenantId, T id) {
        super(systemContext, tenantId, id);
    }

    @Override
    protected void logLifecycleEvent(ComponentLifecycleEvent event, Exception e) {
        super.logLifecycleEvent(event, e);
        if (e instanceof JnksIotRuleNodeUpdateException || (event == ComponentLifecycleEvent.STARTED && e != null)) {
            return;
        }
        processNotificationRule(event, e);
    }

    @Override
    public void destroy(JnksIotActorStopReason stopReason, Throwable cause) {
        super.destroy(stopReason, cause);
        if (stopReason == JnksIotActorStopReason.INIT_FAILED && cause != null) {
            processNotificationRule(ComponentLifecycleEvent.STARTED, cause);
        }
    }

    private void processNotificationRule(ComponentLifecycleEvent event, Throwable e) {
        if (processor == null || systemContext.getNotificationRuleProcessor() == null) {
            return;
        }
        systemContext.getNotificationRuleProcessor().process(RuleEngineComponentLifecycleEventTrigger.builder()
                .tenantId(tenantId)
                .ruleChainId(getRuleChainId())
                .ruleChainName(getRuleChainName())
                .componentId(id)
                .componentName(processor.getComponentName())
                .eventType(event)
                .error(e)
                .build());
    }

    protected abstract RuleChainId getRuleChainId();

    protected abstract String getRuleChainName();

}
