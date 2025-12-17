/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.actors.tenant;

import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.actors.ActorSystemContext;
import org.thingsboard.server.actors.ProcessFailureStrategy;
import org.thingsboard.server.actors.TbActor;
import org.thingsboard.server.actors.TbActorCtx;
import org.thingsboard.server.actors.TbActorException;
import org.thingsboard.server.actors.TbActorId;
import org.thingsboard.server.actors.TbActorNotRegisteredException;
import org.thingsboard.server.actors.TbActorRef;
import org.thingsboard.server.actors.TbEntityActorId;
import org.thingsboard.server.actors.TbEntityTypeActorIdPredicate;
import org.thingsboard.server.actors.TbStringActorId;
import org.thingsboard.server.actors.calculatedField.CalculatedFieldManagerActorCreator;
import org.thingsboard.server.actors.calculatedField.CalculatedFieldStateRestoreMsg;
import org.thingsboard.server.actors.device.DeviceActorCreator;
import org.thingsboard.server.actors.ruleChain.RuleChainManagerActor;
import org.thingsboard.server.actors.service.ContextBasedCreator;
import org.thingsboard.server.actors.service.DefaultActorService;
import org.thingsboard.server.common.data.ApiUsageState;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.Tenant;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.RuleChainId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.plugin.ComponentLifecycleEvent;
import org.thingsboard.server.common.data.rule.RuleChain;
import org.thingsboard.server.common.data.rule.RuleChainType;
import org.thingsboard.server.common.msg.MsgType;
import org.thingsboard.server.common.msg.TbActorMsg;
import org.thingsboard.server.common.msg.TbActorStopReason;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.ToCalculatedFieldSystemMsg;
import org.thingsboard.server.common.msg.aware.DeviceAwareMsg;
import org.thingsboard.server.common.msg.aware.RuleChainAwareMsg;
import org.thingsboard.server.common.msg.cf.CalculatedFieldCacheInitMsg;
import org.thingsboard.server.common.msg.cf.CalculatedFieldEntityLifecycleMsg;
import org.thingsboard.server.common.msg.plugin.ComponentLifecycleMsg;
import org.thingsboard.server.common.msg.queue.PartitionChangeMsg;
import org.thingsboard.server.common.msg.queue.QueueToRuleEngineMsg;
import org.thingsboard.server.common.msg.queue.RuleEngineException;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.common.msg.rule.engine.DeviceDeleteMsg;
import org.thingsboard.server.service.transport.msg.TransportToDeviceActorMsgWrapper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class TenantActor extends RuleChainManagerActor {

    private boolean isRuleEngine;
    private boolean isCore;
    private ApiUsageState apiUsageState;
    private Set<DeviceId> deletedDevices;
    private TbActorRef cfActor;

    private TenantActor(ActorSystemContext systemContext, TenantId tenantId) {
        super(systemContext, tenantId);
        this.deletedDevices = new HashSet<>();
    }

    boolean cantFindTenant = false;

    @Override
    public void init(TbActorCtx ctx) throws TbActorException {
        super.init(ctx);
        log.debug("[{}] Starting tenant actor.", tenantId);
        try {
            // 获取对应的租户信息
            Tenant tenant = systemContext.getTenantService().findTenantById(tenantId);
            if (tenant == null) {
                // 标记不能找到租户
                cantFindTenant = true;
                log.info("[{}] Started tenant actor for missing tenant.", tenantId);
            } else {
                isCore = systemContext.getServiceInfoProvider().isService(ServiceType.TB_CORE);
                isRuleEngine = systemContext.getServiceInfoProvider().isService(ServiceType.TB_RULE_ENGINE);
                if (isRuleEngine) {
                    // 如果当前服务管理该租户
                    if (systemContext.getPartitionService().isManagedByCurrentService(tenantId)) {
                        try {
                            // 获取对应id的子actor，如果不存在则创建计算字段管理器Actor
                            //TODO: IM - extend API usage to have CF Exec Enabled? Not in 4.0;
                            cfActor = ctx.getOrCreateChildActor(new TbStringActorId("CFM|" + tenantId),
                                    () -> DefaultActorService.CF_MANAGER_DISPATCHER_NAME,
                                    () -> new CalculatedFieldManagerActorCreator(systemContext, tenantId),
                                    () -> true);
                            // 初始化计算字段缓存
                            cfActor.tellWithHighPriority(new CalculatedFieldCacheInitMsg(tenantId));
                        } catch (Exception e) {
                            log.info("[{}] Failed to init CF Actor.", tenantId, e);
                        }
                        try {
                            if (getApiUsageState().isReExecEnabled()) {
                                // 如果api使用状态中的执行引擎执行状态是启用的，初始化规则链
                                log.debug("[{}] Going to init rule chains", tenantId);
                                initRuleChains();
                            } else {
                                log.info("[{}] Skip init of the rule chains due to API limits", tenantId);
                            }
                        } catch (Exception e) {
                            log.info("Failed to check ApiUsage \"ReExecEnabled\"!!!", e);
                            cantFindTenant = true;
                        }
                    } else {
                        log.info("Tenant {} is not managed by current service, skipping rule chains and cf actor init", tenantId);
                    }
                }
                log.debug("[{}] Tenant actor started.", tenantId);
            }
        } catch (Exception e) {
            log.warn("[{}] Unknown failure", tenantId, e);
        }
    }

    @Override
    public void destroy(TbActorStopReason stopReason, Throwable cause) {
        log.info("[{}] Stopping tenant actor.", tenantId);
        if (cfActor != null) {
            ctx.stop(cfActor.getActorId());
            cfActor = null;
        }
    }

    @Override
    protected boolean doProcess(TbActorMsg msg) {
        if (cantFindTenant) {
            // 如果找不到租户，则直接返回执行成功（其中有两种类型会执行回调函数）
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
        // 找到该租户，判断消息类型进行分发消息
        switch (msg.getMsgType()) {
            case PARTITION_CHANGE_MSG:
                // 分区变更消息
                onPartitionChangeMsg((PartitionChangeMsg) msg);
                break;
            case COMPONENT_LIFE_CYCLE_MSG:
                // 组件生命周期消息
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
            case DEVICE_EDGE_UPDATE_TO_DEVICE_ACTOR_MSG:
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
            if (msg instanceof CalculatedFieldStateRestoreMsg) {
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
        return systemContext.resolve(ServiceType.TB_CORE, tenantId, entityId).isMyPartition();
    }

    private void onQueueToRuleEngineMsg(QueueToRuleEngineMsg msg) {
        if (!isRuleEngine) {
            // 如果当前启动的不是规则引擎服务，则直接返回
            log.warn("RECEIVED INVALID MESSAGE: {}", msg);
            return;
        }
        TbMsg tbMsg = msg.getMsg();
        if (getApiUsageState().isReExecEnabled()) {
            // 如果启用了规则引擎服务，则根据消息中的规则链ID进行分发
            if (tbMsg.getRuleChainId() == null) {
                // 如果消息没指定规则链ID，则使用根规则链进行分发
                if (getRootChainActor() != null) {
                    getRootChainActor().tell(msg);
                } else {
                    tbMsg.getCallback().onFailure(new RuleEngineException("No Root Rule Chain available!"));
                    log.info("[{}] No Root Chain: {}", tenantId, msg);
                }
            } else {
                try {
                    ctx.tell(new TbEntityActorId(tbMsg.getRuleChainId()), msg);
                } catch (TbActorNotRegisteredException ex) {
                    log.trace("Received message for non-existing rule chain: [{}]", tbMsg.getRuleChainId());
                    //TODO: 3.1 Log it to dead letters queue;
                    tbMsg.getCallback().onSuccess();
                }
            }
        } else {
            log.trace("[{}] Ack message because Rule Engine is disabled", tenantId);
            tbMsg.getCallback().onSuccess();
        }
    }

    private void onRuleChainMsg(RuleChainAwareMsg msg) {
        if (getApiUsageState().isReExecEnabled()) {
            getOrCreateActor(msg.getRuleChainId()).tell(msg);
        }
    }

    private void onToDeviceActorMsg(DeviceAwareMsg msg, boolean priority) {
        if (!isCore) {
            log.warn("RECEIVED INVALID MESSAGE: {}", msg);
        }
        if (deletedDevices.contains(msg.getDeviceId())) {
            // 如果设备已经删除，则直接返回
            log.debug("RECEIVED MESSAGE FOR DELETED DEVICE: {}", msg);
            return;
        }
        TbActorRef deviceActor = getOrCreateDeviceActor(msg.getDeviceId());
        if (priority) {
            deviceActor.tellWithHighPriority(msg);
        } else {
            deviceActor.tell(msg);
        }
    }

    private void onPartitionChangeMsg(PartitionChangeMsg msg) {
        ServiceType serviceType = msg.getServiceType();
        if (ServiceType.TB_RULE_ENGINE.equals(serviceType)) {
            if (systemContext.getPartitionService().isManagedByCurrentService(tenantId)) {
                if (cfActor == null) {
                    try {
                        //TODO: IM - extend API usage to have CF Exec Enabled? Not in 4.0;
                        cfActor = ctx.getOrCreateChildActor(new TbStringActorId("CFM|" + tenantId),
                                () -> DefaultActorService.CF_MANAGER_DISPATCHER_NAME,
                                () -> new CalculatedFieldManagerActorCreator(systemContext, tenantId),
                                () -> true);
                        cfActor.tellWithHighPriority(new CalculatedFieldCacheInitMsg(tenantId));
                    } catch (Exception e) {
                        log.info("[{}] Failed to init CF Actor.", tenantId, e);
                    }
                }
                if (!ruleChainsInitialized) {
                    log.info("Tenant {} is now managed by this service, initializing rule chains", tenantId);
                    initRuleChains();
                }
            } else {
                if (cfActor != null) {
                    ctx.stop(cfActor.getActorId());
                    cfActor = null;
                }
                if (ruleChainsInitialized) {
                    log.info("Tenant {} is no longer managed by this service, stopping rule chains", tenantId);
                    destroyRuleChains();
                }
                return;
            }

            // 往当前tenantActor中的子ruleChainActor广播数据
            broadcast(msg);
        } else if (ServiceType.TB_CORE.equals(serviceType)) {
            // 停止当前tenantActor中的子deviceActor
            List<TbActorId> deviceActorIds = ctx.filterChildren(new TbEntityTypeActorIdPredicate(EntityType.DEVICE) {
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
            // 如果实体类型是API_USAGE_STATE，则更新apiUsageState对象
            ApiUsageState old = getApiUsageState();
            apiUsageState = new ApiUsageState(systemContext.getApiUsageStateService().getApiUsageState(tenantId));
            if (old.isReExecEnabled() && !apiUsageState.isReExecEnabled()) {
                // 如果旧的apiUsageState对象执行引擎执行状态是启用的，而新的apiUsageState对象没有启用，则销毁规则链
                log.info("[{}] Received API state update. Going to DISABLE Rule Engine execution.", tenantId);
                destroyRuleChains();
            } else if (!old.isReExecEnabled() && apiUsageState.isReExecEnabled()) {
                // 如果旧的apiUsageState对象执行引擎执行状态是没有启用的，而新的apiUsageState对象启用，则初始化
                log.info("[{}] Received API state update. Going to ENABLE Rule Engine execution.", tenantId);
                initRuleChains();
            }
        }
        if (msg.getEntityId().getEntityType() == EntityType.DEVICE && ComponentLifecycleEvent.DELETED == msg.getEvent() && isMyPartition(msg.getEntityId())) {
            // 当设备被删除时，并且设备是当前服务负责的。则向对应的deviceActor发送删除的数据，并将设备ID添加到deletedDevices列表中
            DeviceId deviceId = (DeviceId) msg.getEntityId();
            onToDeviceActorMsg(new DeviceDeleteMsg(tenantId, deviceId), true);
            deletedDevices.add(deviceId);
        }
        if (isRuleEngine) {
            if (ruleChainsInitialized) {
                // 如果是规则引擎服务并且已经初始化了规则链，则向对应的ruleChainActor发送数据
                TbActorRef target = getEntityActorRef(msg.getEntityId());
                if (target != null) {
                    if (msg.getEntityId().getEntityType() == EntityType.RULE_CHAIN) {
                        RuleChain ruleChain = systemContext.getRuleChainService().
                                findRuleChainById(tenantId, new RuleChainId(msg.getEntityId().getId()));
                        if (ruleChain != null && RuleChainType.CORE.equals(ruleChain.getType())) {
                            visit(ruleChain, target);
                        }
                    }
                    target.tellWithHighPriority(msg);
                } else {
                    log.debug("[{}] Invalid component lifecycle msg: {}", tenantId, msg);
                }
            }
            if (cfActor != null) {
                // 如果实体类型是计算字段、设备或资产，则向对应的calculatedFieldManagerActor发送数据
                if (msg.getEntityId().getEntityType().isOneOf(EntityType.CALCULATED_FIELD, EntityType.DEVICE, EntityType.ASSET)) {
                    cfActor.tellWithHighPriority(new CalculatedFieldEntityLifecycleMsg(tenantId, msg));
                }
            }
        }
    }

    private TbActorRef getOrCreateDeviceActor(DeviceId deviceId) {
        return ctx.getOrCreateChildActor(new TbEntityActorId(deviceId),
                () -> DefaultActorService.DEVICE_DISPATCHER_NAME,
                () -> new DeviceActorCreator(systemContext, tenantId, deviceId),
                () -> true);
    }

    private ApiUsageState getApiUsageState() {
        if (apiUsageState == null) {
            apiUsageState = new ApiUsageState(systemContext.getApiUsageStateService().getApiUsageState(tenantId));
        }
        return apiUsageState;
    }

    @Override
    public ProcessFailureStrategy onProcessFailure(TbActorMsg msg, Throwable t) {
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
        public TbActorId createActorId() {
            return new TbEntityActorId(tenantId);
        }

        @Override
        public TbActor createActor() {
            return new TenantActor(context, tenantId);
        }
    }

}
