package com.jnks.iot.server.actors.tenant;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.ProcessFailureStrategy;
import com.jnks.iot.server.actors.JnksIotActor;
import com.jnks.iot.server.actors.JnksIotActorCtx;
import com.jnks.iot.server.actors.JnksIotActorException;
import com.jnks.iot.server.actors.JnksIotActorId;
import com.jnks.iot.server.actors.JnksIotActorNotRegisteredException;
import com.jnks.iot.server.actors.JnksIotActorRef;
import com.jnks.iot.server.actors.JnksIotEntityActorId;
import com.jnks.iot.server.actors.JnksIotEntityTypeActorIdPredicate;
import com.jnks.iot.server.actors.service.ContextAwareActor;
import com.jnks.iot.server.actors.service.ContextBasedCreator;
import com.jnks.iot.server.common.data.ApiUsageState;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.Tenant;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.plugin.ComponentLifecycleEvent;
import com.jnks.iot.server.common.data.rule.RuleChain;
import com.jnks.iot.server.common.data.rule.RuleChainType;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.JnksIotActorMsg;
import com.jnks.iot.server.common.msg.JnksIotActorStopReason;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.ToCalculatedFieldSystemMsg;
import com.jnks.iot.server.common.msg.aware.DeviceAwareMsg;
import com.jnks.iot.server.common.msg.aware.RuleChainAwareMsg;
import com.jnks.iot.server.common.msg.cf.CalculatedFieldCacheInitMsg;
import com.jnks.iot.server.common.msg.cf.CalculatedFieldEntityLifecycleMsg;
import com.jnks.iot.server.common.msg.plugin.ComponentLifecycleMsg;
import com.jnks.iot.server.common.msg.queue.PartitionChangeMsg;
import com.jnks.iot.server.common.msg.queue.QueueToRuleEngineMsg;
import com.jnks.iot.server.common.msg.queue.RuleEngineException;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.common.msg.rule.engine.DeviceDeleteMsg;
import com.jnks.iot.server.service.transport.msg.TransportToDeviceActorMsgWrapper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class TenantActor extends ContextAwareActor {

    private final TenantId tenantId;
    private final TenantDeviceActorSupport deviceActorSupport;
    private final TenantRuleEngineActorSupport ruleEngineActorSupport;

    private boolean isRuleEngine;
    private boolean isCore;
    private boolean cantFindTenant;
    private boolean ruleChainsInitialized;

    private ApiUsageState apiUsageState;
    private final Set<DeviceId> deletedDevices;
    private JnksIotActorRef cfActor;

    private TenantActor(ActorSystemContext systemContext, TenantId tenantId) {
        super(systemContext);
        this.tenantId = tenantId;
        this.deviceActorSupport = systemContext.getTenantDeviceActorSupport();
        this.ruleEngineActorSupport = systemContext.getTenantRuleEngineActorSupportFactory() != null
                ? systemContext.getTenantRuleEngineActorSupportFactory().create(systemContext, tenantId)
                : null;
        this.deletedDevices = new HashSet<>();
    }

    @Override
    public void init(JnksIotActorCtx ctx) throws JnksIotActorException {
        super.init(ctx);
        log.debug("[{}] Starting tenant actor.", tenantId);
        try {
            Tenant tenant = systemContext.getTenantService().findTenantById(tenantId);
            if (tenant == null) {
                cantFindTenant = true;
                log.info("[{}] Started tenant actor for missing tenant.", tenantId);
                return;
            }
            isCore = systemContext.getServiceInfoProvider().isService(ServiceType.JNKS_IOT_CORE);
            isRuleEngine = systemContext.getServiceInfoProvider().isService(ServiceType.JNKS_IOT_RULE_ENGINE);
            if (isRuleEngine && systemContext.getPartitionService().isManagedByCurrentService(tenantId)) {
                try {
                    cfActor = getOrCreateCalculatedFieldManagerActor();
                    if (cfActor != null) {
                        cfActor.tellWithHighPriority(new CalculatedFieldCacheInitMsg(tenantId));
                    }
                } catch (Exception e) {
                    log.info("[{}] Failed to init CF Actor.", tenantId, e);
                }
                try {
                    if (getApiUsageState().isReExecEnabled()) {
                        log.debug("[{}] Going to init rule chains", tenantId);
                        initRuleChainsIfSupported();
                    } else {
                        log.info("[{}] Skip init of the rule chains due to API limits", tenantId);
                    }
                } catch (Exception e) {
                    log.info("Failed to check ApiUsage \"ReExecEnabled\"!!!", e);
                    cantFindTenant = true;
                }
            }
            log.debug("[{}] Tenant actor started.", tenantId);
        } catch (Exception e) {
            log.warn("[{}] Unknown failure", tenantId, e);
        }
    }

    @Override
    public void destroy(JnksIotActorStopReason stopReason, Throwable cause) {
        log.info("[{}] Stopping tenant actor.", tenantId);
        if (cfActor != null) {
            ctx.stop(cfActor.getActorId());
            cfActor = null;
        }
    }

    @Override
    protected boolean doProcess(JnksIotActorMsg msg) {
        if (cantFindTenant) {
            log.info("[{}] Processing missing Tenant msg: {}", tenantId, msg);
            if (msg.getMsgType().equals(MsgType.QUEUE_TO_RULE_ENGINE_MSG)) {
                QueueToRuleEngineMsg queueMsg = (QueueToRuleEngineMsg) msg;
                queueMsg.getMsg().getCallback().onSuccess();
            } else if (msg.getMsgType().equals(MsgType.TRANSPORT_TO_DEVICE_ACTOR_MSG)) {
                TransportToDeviceActorMsgWrapper transportMsg = (TransportToDeviceActorMsgWrapper) msg;
                transportMsg.getCallback().onSuccess();
            }
            return true;
        }
        switch (msg.getMsgType()) {
            case PARTITION_CHANGE_MSG:
                onPartitionChangeMsg((PartitionChangeMsg) msg);
                break;
            case COMPONENT_LIFE_CYCLE_MSG:
                onComponentLifecycleMsg((ComponentLifecycleMsg) msg);
                break;
            case QUEUE_TO_RULE_ENGINE_MSG:
                onQueueToRuleEngineMsg((QueueToRuleEngineMsg) msg);
                break;
            case TRANSPORT_TO_DEVICE_ACTOR_MSG:
                onToDeviceActorMsg((DeviceAwareMsg) msg, false);
                break;
            case DEVICE_ATTRIBUTES_UPDATE_TO_DEVICE_ACTOR_MSG:
            case DEVICE_CREDENTIALS_UPDATE_TO_DEVICE_ACTOR_MSG:
            case DEVICE_NAME_OR_TYPE_UPDATE_TO_DEVICE_ACTOR_MSG:
            case DEVICE_RPC_REQUEST_TO_DEVICE_ACTOR_MSG:
            case DEVICE_RPC_RESPONSE_TO_DEVICE_ACTOR_MSG:
            case SERVER_RPC_RESPONSE_TO_DEVICE_ACTOR_MSG:
            case REMOVE_RPC_TO_DEVICE_ACTOR_MSG:
                onToDeviceActorMsg((DeviceAwareMsg) msg, true);
                break;
            case SESSION_TIMEOUT_MSG:
                ctx.broadcastToChildrenByType(msg, EntityType.DEVICE);
                break;
            case RULE_CHAIN_INPUT_MSG:
            case RULE_CHAIN_OUTPUT_MSG:
            case RULE_CHAIN_TO_RULE_CHAIN_MSG:
                onRuleChainMsg((RuleChainAwareMsg) msg);
                break;
            case CF_CACHE_INIT_MSG:
            case CF_INIT_PROFILE_ENTITY_MSG:
            case CF_INIT_MSG:
            case CF_LINK_INIT_MSG:
            case CF_STATE_RESTORE_MSG:
            case CF_PARTITIONS_CHANGE_MSG:
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

    private void onToCalculatedFieldSystemActorMsg(ToCalculatedFieldSystemMsg msg, boolean priority) {
        if (cfActor == null) {
            if (msg.getMsgType() == MsgType.CF_STATE_RESTORE_MSG) {
                log.warn("[{}] CF Actor is not initialized. ToCalculatedFieldSystemMsg: [{}]", tenantId, msg);
            } else {
                log.debug("[{}] CF Actor is not initialized. ToCalculatedFieldSystemMsg: [{}]", tenantId, msg);
            }
            msg.getCallback().onSuccess();
            return;
        }
        if (priority) {
            cfActor.tellWithHighPriority(msg);
        } else {
            cfActor.tell(msg);
        }
    }

    private boolean isMyPartition(EntityId entityId) {
        return systemContext.resolve(ServiceType.JNKS_IOT_CORE, tenantId, entityId).isMyPartition();
    }

    private void onQueueToRuleEngineMsg(QueueToRuleEngineMsg msg) {
        if (!isRuleEngine || ruleEngineActorSupport == null) {
            log.warn("RECEIVED INVALID MESSAGE: {}", msg);
            return;
        }
        JnksIotMsg jnksIotMsg = msg.getMsg();
        if (getApiUsageState().isReExecEnabled()) {
            if (jnksIotMsg.getRuleChainId() == null) {
                JnksIotActorRef rootChainActor = ruleEngineActorSupport.getRootChainActor();
                if (rootChainActor != null) {
                    rootChainActor.tell(msg);
                } else {
                    jnksIotMsg.getCallback().onFailure(new RuleEngineException("No Root Rule Chain available!"));
                    log.info("[{}] No Root Chain: {}", tenantId, msg);
                }
            } else {
                try {
                    ctx.tell(new JnksIotEntityActorId(jnksIotMsg.getRuleChainId()), msg);
                } catch (JnksIotActorNotRegisteredException ex) {
                    log.trace("Received message for non-existing rule chain: [{}]", jnksIotMsg.getRuleChainId());
                    jnksIotMsg.getCallback().onSuccess();
                }
            }
        } else {
            log.trace("[{}] Ack message because Rule Engine is disabled", tenantId);
            jnksIotMsg.getCallback().onSuccess();
        }
    }

    private void onRuleChainMsg(RuleChainAwareMsg msg) {
        if (getApiUsageState().isReExecEnabled() && ruleEngineActorSupport != null) {
            ruleEngineActorSupport.getOrCreateRuleChainActor(ctx, msg.getRuleChainId()).tell(msg);
        }
    }

    private void onToDeviceActorMsg(DeviceAwareMsg msg, boolean priority) {
        if (!isCore) {
            log.warn("RECEIVED INVALID MESSAGE: {}", msg);
        }
        if (deletedDevices.contains(msg.getDeviceId())) {
            log.debug("RECEIVED MESSAGE FOR DELETED DEVICE: {}", msg);
            return;
        }
        JnksIotActorRef deviceActor = getOrCreateDeviceActor(msg.getDeviceId());
        if (deviceActor == null) {
            log.warn("[{}] Device actor support is not configured. Device id: {}", tenantId, msg.getDeviceId());
            return;
        }
        if (priority) {
            deviceActor.tellWithHighPriority(msg);
        } else {
            deviceActor.tell(msg);
        }
    }

    private void onPartitionChangeMsg(PartitionChangeMsg msg) {
        ServiceType serviceType = msg.getServiceType();
        if (ServiceType.JNKS_IOT_RULE_ENGINE.equals(serviceType)) {
            if (systemContext.getPartitionService().isManagedByCurrentService(tenantId)) {
                if (cfActor == null) {
                    try {
                        cfActor = getOrCreateCalculatedFieldManagerActor();
                        if (cfActor != null) {
                            cfActor.tellWithHighPriority(new CalculatedFieldCacheInitMsg(tenantId));
                        }
                    } catch (Exception e) {
                        log.info("[{}] Failed to init CF Actor.", tenantId, e);
                    }
                }
                if (!ruleChainsInitialized) {
                    log.info("Tenant {} is now managed by this service, initializing rule chains", tenantId);
                    initRuleChainsIfSupported();
                }
            } else {
                if (cfActor != null) {
                    ctx.stop(cfActor.getActorId());
                    cfActor = null;
                }
                if (ruleChainsInitialized) {
                    log.info("Tenant {} is no longer managed by this service, stopping rule chains", tenantId);
                    destroyRuleChainsIfSupported();
                }
                return;
            }
            if (ruleEngineActorSupport != null) {
                ruleEngineActorSupport.broadcastToRuleChains(ctx, msg);
            }
        } else if (ServiceType.JNKS_IOT_CORE.equals(serviceType)) {
            List<JnksIotActorId> deviceActorIds = ctx.filterChildren(new JnksIotEntityTypeActorIdPredicate(EntityType.DEVICE) {
                @Override
                protected boolean testEntityId(EntityId entityId) {
                    return super.testEntityId(entityId) && !isMyPartition(entityId);
                }
            });
            deviceActorIds.forEach(id -> ctx.stop(id));
        }
    }

    private void onComponentLifecycleMsg(ComponentLifecycleMsg msg) {
        if (msg.getEntityId().getEntityType().equals(EntityType.API_USAGE_STATE)) {
            ApiUsageState old = getApiUsageState();
            apiUsageState = new ApiUsageState(systemContext.getApiUsageStateService().getApiUsageState(tenantId));
            if (old.isReExecEnabled() && !apiUsageState.isReExecEnabled()) {
                log.info("[{}] Received API state update. Going to DISABLE Rule Engine execution.", tenantId);
                destroyRuleChainsIfSupported();
            } else if (!old.isReExecEnabled() && apiUsageState.isReExecEnabled()) {
                log.info("[{}] Received API state update. Going to ENABLE Rule Engine execution.", tenantId);
                initRuleChainsIfSupported();
            }
        }
        if (msg.getEntityId().getEntityType() == EntityType.DEVICE
                && ComponentLifecycleEvent.DELETED == msg.getEvent()
                && isMyPartition(msg.getEntityId())) {
            DeviceId deviceId = (DeviceId) msg.getEntityId();
            onToDeviceActorMsg(new DeviceDeleteMsg(tenantId, deviceId), true);
            deletedDevices.add(deviceId);
        }
        if (isRuleEngine && ruleEngineActorSupport != null) {
            if (ruleChainsInitialized) {
                JnksIotActorRef target = ruleEngineActorSupport.getEntityActorRef(ctx, msg.getEntityId());
                if (target != null) {
                    if (msg.getEntityId().getEntityType() == EntityType.RULE_CHAIN) {
                        RuleChain ruleChain = systemContext.getRuleChainService()
                                .findRuleChainById(tenantId, new RuleChainId(msg.getEntityId().getId()));
                        if (ruleChain != null && RuleChainType.CORE.equals(ruleChain.getType())) {
                            ruleEngineActorSupport.visit(ruleChain, target);
                        }
                    }
                    target.tellWithHighPriority(msg);
                } else {
                    log.debug("[{}] Invalid component lifecycle msg: {}", tenantId, msg);
                }
            }
            if (cfActor != null && msg.getEntityId().getEntityType().isOneOf(EntityType.CALCULATED_FIELD, EntityType.DEVICE, EntityType.ASSET)) {
                cfActor.tellWithHighPriority(new CalculatedFieldEntityLifecycleMsg(tenantId, msg));
            }
        }
    }

    private JnksIotActorRef getOrCreateCalculatedFieldManagerActor() {
        return ruleEngineActorSupport != null ? ruleEngineActorSupport.getOrCreateCalculatedFieldManagerActor(ctx) : null;
    }

    private void initRuleChainsIfSupported() {
        if (ruleEngineActorSupport != null) {
            ruleEngineActorSupport.initRuleChains(ctx);
            ruleChainsInitialized = ruleEngineActorSupport.isRuleChainsInitialized();
        }
    }

    private void destroyRuleChainsIfSupported() {
        if (ruleEngineActorSupport != null) {
            ruleEngineActorSupport.destroyRuleChains(ctx);
            ruleChainsInitialized = ruleEngineActorSupport.isRuleChainsInitialized();
        } else {
            ruleChainsInitialized = false;
        }
    }

    private JnksIotActorRef getOrCreateDeviceActor(DeviceId deviceId) {
        return deviceActorSupport != null ? deviceActorSupport.getOrCreateDeviceActor(ctx, tenantId, deviceId) : null;
    }

    private ApiUsageState getApiUsageState() {
        if (apiUsageState == null) {
            apiUsageState = new ApiUsageState(systemContext.getApiUsageStateService().getApiUsageState(tenantId));
        }
        return apiUsageState;
    }

    @Override
    public ProcessFailureStrategy onProcessFailure(JnksIotActorMsg msg, Throwable t) {
        log.error("[{}] Failed to process msg: {}", tenantId, msg, t);
        return doProcessFailure(t);
    }

    public static class ActorCreator extends ContextBasedCreator {

        private final TenantId tenantId;

        public ActorCreator(ActorSystemContext context, TenantId tenantId) {
            super(context);
            this.tenantId = tenantId;
        }

        @Override
        public JnksIotActorId createActorId() {
            return new JnksIotEntityActorId(tenantId);
        }

        @Override
        public JnksIotActor createActor() {
            return new TenantActor(context, tenantId);
        }
    }
}
