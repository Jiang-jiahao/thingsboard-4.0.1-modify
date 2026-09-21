package com.jnks.iot.server.actors.app;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.ProcessFailureStrategy;
import com.jnks.iot.server.actors.JnksIotActor;
import com.jnks.iot.server.actors.JnksIotActorCtx;
import com.jnks.iot.server.actors.JnksIotActorException;
import com.jnks.iot.server.actors.JnksIotActorId;
import com.jnks.iot.server.actors.JnksIotActorRef;
import com.jnks.iot.server.actors.JnksIotEntityActorId;
import com.jnks.iot.server.actors.service.ContextAwareActor;
import com.jnks.iot.server.actors.service.ContextBasedCreator;
import com.jnks.iot.server.actors.service.DefaultActorService;
import com.jnks.iot.server.actors.shared.SessionTimeoutCheckMsg;
import com.jnks.iot.server.actors.tenant.TenantActor;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.Tenant;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageDataIterable;
import com.jnks.iot.server.common.data.plugin.ComponentLifecycleEvent;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.JnksIotActorMsg;
import com.jnks.iot.server.common.msg.ToCalculatedFieldSystemMsg;
import com.jnks.iot.server.common.msg.aware.TenantAwareMsg;
import com.jnks.iot.server.common.msg.plugin.ComponentLifecycleMsg;
import com.jnks.iot.server.common.msg.queue.QueueToRuleEngineMsg;
import com.jnks.iot.server.common.msg.queue.RuleEngineException;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.dao.tenant.TenantService;
import com.jnks.iot.server.service.transport.msg.TransportToDeviceActorMsgWrapper;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

@Slf4j
public class AppActor extends ContextAwareActor {

    private final TenantService tenantService;
    private final Set<TenantId> deletedTenants;
    private volatile boolean ruleChainsInitialized;

    private AppActor(ActorSystemContext systemContext) {
        super(systemContext);
        this.tenantService = systemContext.getTenantService();
        this.deletedTenants = new HashSet<>();
    }

    @Override
    public void init(JnksIotActorCtx ctx) throws JnksIotActorException {
        super.init(ctx);
        if (systemContext.getServiceInfoProvider().isService(ServiceType.JNKS_IOT_CORE)) {
            systemContext.schedulePeriodicMsgWithDelay(ctx, SessionTimeoutCheckMsg.instance(),
                    systemContext.getSessionReportTimeout(), systemContext.getSessionReportTimeout());
        }
    }

    @Override
    protected boolean doProcess(JnksIotActorMsg msg) {
        if (!ruleChainsInitialized) {
            if (MsgType.APP_INIT_MSG.equals(msg.getMsgType())) {
                // 初始化所有租户Actor
                initTenantActors();
                ruleChainsInitialized = true;
            } else {
                if (!msg.getMsgType().isIgnoreOnStart()) {
                    log.warn("Attempt to initialize Rule Chains by unexpected message: {}", msg);
                }
                return true;
            }
        }
        switch (msg.getMsgType()) {
            case APP_INIT_MSG:
                break;
            case PARTITION_CHANGE_MSG:
            case CF_PARTITIONS_CHANGE_MSG:
                ctx.broadcastToChildren(msg, true);
                break;
            case COMPONENT_LIFE_CYCLE_MSG:
                onComponentLifecycleMsg((ComponentLifecycleMsg) msg);
                break;
            case QUEUE_TO_RULE_ENGINE_MSG:
                onQueueToRuleEngineMsg((QueueToRuleEngineMsg) msg);
                break;
            case TRANSPORT_TO_DEVICE_ACTOR_MSG:
                onToDeviceActorMsg((TenantAwareMsg) msg, false);
                break;
            case DEVICE_ATTRIBUTES_UPDATE_TO_DEVICE_ACTOR_MSG:
            case DEVICE_CREDENTIALS_UPDATE_TO_DEVICE_ACTOR_MSG:
            case DEVICE_NAME_OR_TYPE_UPDATE_TO_DEVICE_ACTOR_MSG:
            case DEVICE_RPC_REQUEST_TO_DEVICE_ACTOR_MSG:
            case DEVICE_RPC_RESPONSE_TO_DEVICE_ACTOR_MSG:
            case SERVER_RPC_RESPONSE_TO_DEVICE_ACTOR_MSG:
            case REMOVE_RPC_TO_DEVICE_ACTOR_MSG:
                onToDeviceActorMsg((TenantAwareMsg) msg, true);
                break;
            case SESSION_TIMEOUT_MSG:
                ctx.broadcastToChildrenByType(msg, EntityType.TENANT);
                break;
            case CF_CACHE_INIT_MSG:
            case CF_INIT_PROFILE_ENTITY_MSG:
            case CF_INIT_MSG:
            case CF_LINK_INIT_MSG:
            case CF_STATE_RESTORE_MSG:
                //TODO: use priority from the message body. For example, messages about CF lifecycle are important and Device lifecycle are not.
                //      same for the Linked telemetry.
                onToCalculatedFieldSystemActorMsg((ToCalculatedFieldSystemMsg) msg, true);
                break;
            case CF_TELEMETRY_MSG:
            case CF_LINKED_TELEMETRY_MSG:
                onToCalculatedFieldSystemActorMsg((ToCalculatedFieldSystemMsg) msg, false);
                break;
            default:
                return false;
        }
        return true;
    }

    private void initTenantActors() {
        log.info("Starting main system actor.");
        try {
            if (systemContext.isTenantComponentsInitEnabled()) {
                PageDataIterable<Tenant> tenantIterator = new PageDataIterable<>(tenantService::findTenants, ENTITY_PACK_LIMIT);
                for (Tenant tenant : tenantIterator) {
                    log.debug("[{}] Creating tenant actor", tenant.getId());
                    getOrCreateTenantActor(tenant.getId()).ifPresentOrElse(tenantActor -> {
                        log.debug("[{}] Tenant actor created.", tenant.getId());
                    }, () -> {
                        log.debug("[{}] Skipped actor creation", tenant.getId());
                    });
                }
            }
            log.info("Main system actor started.");
        } catch (Exception e) {
            log.warn("Unknown failure", e);
        }
    }

    private void onQueueToRuleEngineMsg(QueueToRuleEngineMsg msg) {
        if (TenantId.SYS_TENANT_ID.equals(msg.getTenantId())) {
            msg.getMsg().getCallback().onFailure(new RuleEngineException("Message has system tenant id!"));
        } else {
            getOrCreateTenantActor(msg.getTenantId()).ifPresentOrElse(actor -> {
                actor.tell(msg);
            }, () -> msg.getMsg().getCallback().onSuccess());
        }
    }

    private void onComponentLifecycleMsg(ComponentLifecycleMsg msg) {
        JnksIotActorRef target = null;
        if (TenantId.SYS_TENANT_ID.equals(msg.getTenantId())) {
            if (!EntityType.TENANT_PROFILE.equals(msg.getEntityId().getEntityType())) {
                log.warn("Message has system tenant id: {}", msg);
            }
        } else {
            if (EntityType.TENANT.equals(msg.getEntityId().getEntityType())) {
                TenantId tenantId = TenantId.fromUUID(msg.getEntityId().getId());
                if (msg.getEvent() == ComponentLifecycleEvent.DELETED) {
                    log.info("[{}] Handling tenant deleted notification: {}", msg.getTenantId(), msg);
                    deletedTenants.add(tenantId);
                    ctx.stop(new JnksIotEntityActorId(tenantId));
                    return;
                }
            }
            target = getOrCreateTenantActor(msg.getTenantId()).orElseGet(() -> {
                log.debug("Ignoring component lifecycle msg for tenant {} because it is not managed by this service", msg.getTenantId());
                return null;
            });
        }
        if (target != null) {
            target.tellWithHighPriority(msg);
        } else {
            log.debug("[{}] Invalid component lifecycle msg: {}", msg.getTenantId(), msg);
        }
    }

    private void onToCalculatedFieldSystemActorMsg(ToCalculatedFieldSystemMsg msg, boolean priority) {
        getOrCreateTenantActor(msg.getTenantId()).ifPresentOrElse(tenantActor -> {
            if (priority) {
                tenantActor.tellWithHighPriority(msg);
            } else {
                tenantActor.tell(msg);
            }
        }, () -> {
            msg.getCallback().onSuccess();
        });
    }


    private void onToDeviceActorMsg(TenantAwareMsg msg, boolean priority) {
        getOrCreateTenantActor(msg.getTenantId()).ifPresentOrElse(tenantActor -> {
            if (priority) {
                tenantActor.tellWithHighPriority(msg);
            } else {
                tenantActor.tell(msg);
            }
        }, () -> {
            if (msg instanceof TransportToDeviceActorMsgWrapper) {
                ((TransportToDeviceActorMsgWrapper) msg).getCallback().onSuccess();
            }
        });
    }

    private Optional<JnksIotActorRef> getOrCreateTenantActor(TenantId tenantId) {
        if (deletedTenants.contains(tenantId)) {
            return Optional.empty();
        }
        return Optional.ofNullable(ctx.getOrCreateChildActor(new JnksIotEntityActorId(tenantId),
                () -> DefaultActorService.TENANT_DISPATCHER_NAME,
                () -> new TenantActor.ActorCreator(systemContext, tenantId),
                () -> true));
    }

    @Override
    public ProcessFailureStrategy onProcessFailure(JnksIotActorMsg msg, Throwable t) {
        log.error("Failed to process msg: {}", msg, t);
        return doProcessFailure(t);
    }

    public static class ActorCreator extends ContextBasedCreator {

        public ActorCreator(ActorSystemContext context) {
            super(context);
        }

        @Override
        public JnksIotActorId createActorId() {
            return new JnksIotEntityActorId(TenantId.SYS_TENANT_ID);
        }

        @Override
        public JnksIotActor createActor() {
            return new AppActor(context);
        }
    }

}
