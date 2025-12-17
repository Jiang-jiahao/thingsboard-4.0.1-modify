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
package org.thingsboard.server.actors.app;

import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.actors.ActorSystemContext;
import org.thingsboard.server.actors.ProcessFailureStrategy;
import org.thingsboard.server.actors.TbActor;
import org.thingsboard.server.actors.TbActorCtx;
import org.thingsboard.server.actors.TbActorException;
import org.thingsboard.server.actors.TbActorId;
import org.thingsboard.server.actors.TbActorRef;
import org.thingsboard.server.actors.TbEntityActorId;
import org.thingsboard.server.actors.device.SessionTimeoutCheckMsg;
import org.thingsboard.server.actors.service.ContextAwareActor;
import org.thingsboard.server.actors.service.ContextBasedCreator;
import org.thingsboard.server.actors.service.DefaultActorService;
import org.thingsboard.server.actors.tenant.TenantActor;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.Tenant;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.page.PageDataIterable;
import org.thingsboard.server.common.data.plugin.ComponentLifecycleEvent;
import org.thingsboard.server.common.msg.MsgType;
import org.thingsboard.server.common.msg.TbActorMsg;
import org.thingsboard.server.common.msg.ToCalculatedFieldSystemMsg;
import org.thingsboard.server.common.msg.aware.TenantAwareMsg;
import org.thingsboard.server.common.msg.plugin.ComponentLifecycleMsg;
import org.thingsboard.server.common.msg.queue.QueueToRuleEngineMsg;
import org.thingsboard.server.common.msg.queue.RuleEngineException;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.dao.tenant.TenantService;
import org.thingsboard.server.service.transport.msg.TransportToDeviceActorMsgWrapper;

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
    public void init(TbActorCtx ctx) throws TbActorException {
        super.init(ctx);
        if (systemContext.getServiceInfoProvider().isService(ServiceType.TB_CORE)) {
            systemContext.schedulePeriodicMsgWithDelay(ctx, SessionTimeoutCheckMsg.instance(),
                    systemContext.getSessionReportTimeout(), systemContext.getSessionReportTimeout());
        }
    }

    @Override
    protected boolean doProcess(TbActorMsg msg) {
        // 如果规则链尚未初始化，需要先处理初始化消息
        if (!ruleChainsInitialized) {
            if (MsgType.APP_INIT_MSG.equals(msg.getMsgType())) {
                // 初始化所有租户Actor
                initTenantActors();
                ruleChainsInitialized = true;
            } else {
                // 在初始化完成前，忽略非初始化消息（某些特定类型除外）
                if (!msg.getMsgType().isIgnoreOnStart()) {
                    log.warn("Attempt to initialize Rule Chains by unexpected message: {}", msg);
                }
                return true;
            }
        }
        // 根据消息类型进行路由处理
        switch (msg.getMsgType()) {
            case APP_INIT_MSG:
                // 应用初始化消息，已在前面处理过
                break;
            case PARTITION_CHANGE_MSG:
            case CF_PARTITIONS_CHANGE_MSG:
                // 分区变更消息：广播给所有子Actor（租户Actor）
                ctx.broadcastToChildren(msg, true);
                break;
            case COMPONENT_LIFE_CYCLE_MSG:
                // 组件生命周期消息：处理组件创建、更新、删除等事件
                onComponentLifecycleMsg((ComponentLifecycleMsg) msg);
                break;
            case QUEUE_TO_RULE_ENGINE_MSG:
                // 队列到规则引擎消息：处理设备数据消息
                onQueueToRuleEngineMsg((QueueToRuleEngineMsg) msg);
                break;
            case TRANSPORT_TO_DEVICE_ACTOR_MSG:
                // 传输到设备Actor消息：处理设备通信消息（非优先）
                onToDeviceActorMsg((TenantAwareMsg) msg, false);
                break;
            case DEVICE_ATTRIBUTES_UPDATE_TO_DEVICE_ACTOR_MSG:
            case DEVICE_CREDENTIALS_UPDATE_TO_DEVICE_ACTOR_MSG:
            case DEVICE_NAME_OR_TYPE_UPDATE_TO_DEVICE_ACTOR_MSG:
            case DEVICE_EDGE_UPDATE_TO_DEVICE_ACTOR_MSG:
            case DEVICE_RPC_REQUEST_TO_DEVICE_ACTOR_MSG:
            case DEVICE_RPC_RESPONSE_TO_DEVICE_ACTOR_MSG:
            case SERVER_RPC_RESPONSE_TO_DEVICE_ACTOR_MSG:
            case REMOVE_RPC_TO_DEVICE_ACTOR_MSG:
                // 各种设备相关消息：处理设备属性、凭证、RPC等（优先处理）
                onToDeviceActorMsg((TenantAwareMsg) msg, true);
                break;
            case SESSION_TIMEOUT_MSG:
                // 会话超时消息：广播给所有租户类型的子Actor
                ctx.broadcastToChildrenByType(msg, EntityType.TENANT);
                break;
            case CF_CACHE_INIT_MSG:
            case CF_INIT_PROFILE_ENTITY_MSG:
            case CF_INIT_MSG:
            case CF_LINK_INIT_MSG:
            case CF_STATE_RESTORE_MSG:
                //TODO: use priority from the message body. For example, messages about CF lifecycle are important and Device lifecycle are not.
                //      same for the Linked telemetry.
                // 计算字段系统消息：初始化、缓存、状态恢复等（优先处理）
                onToCalculatedFieldSystemActorMsg((ToCalculatedFieldSystemMsg) msg, true);
                break;
            case CF_TELEMETRY_MSG:
            case CF_LINKED_TELEMETRY_MSG:
                // 计算字段遥测消息：处理遥测数据（非优先处理）
                onToCalculatedFieldSystemActorMsg((ToCalculatedFieldSystemMsg) msg, false);
                break;
            default:
                return false;
        }
        return true;
    }

    /**
     * 初始化所有租户Actor
     * 系统启动时调用，为每个租户创建对应的TenantActor
     */
    private void initTenantActors() {
        log.info("Starting main system actor.");
        try {
            // 检查是否启用租户组件初始化
            if (systemContext.isTenantComponentsInitEnabled()) {
                // 分页迭代所有租户
                PageDataIterable<Tenant> tenantIterator = new PageDataIterable<>(tenantService::findTenants, ENTITY_PACK_LIMIT);
                for (Tenant tenant : tenantIterator) {
                    log.debug("[{}] Creating tenant actor", tenant.getId());
                    // 获取或创建租户Actor
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

    /**
     * 处理队列到规则引擎的消息
     * 将设备数据消息路由到对应的租户Actor进行处理
     */
    private void onQueueToRuleEngineMsg(QueueToRuleEngineMsg msg) {
        // 系统租户的消息直接失败（系统租户不应该有设备数据）
        if (TenantId.SYS_TENANT_ID.equals(msg.getTenantId())) {
            msg.getMsg().getCallback().onFailure(new RuleEngineException("Message has system tenant id!"));
        } else {
            // 路由到对应租户的Actor处理
            getOrCreateTenantActor(msg.getTenantId()).ifPresentOrElse(actor -> {
                actor.tell(msg);
                // 如果租户Actor不存在，直接标记消息处理成功
                // 这种情况可能发生在租户刚被删除时
            }, () -> msg.getMsg().getCallback().onSuccess());
        }
    }

    /**
     * 处理组件生命周期消息
     * 处理各种实体（租户、设备、资产等）的创建、更新、删除事件
     */
    private void onComponentLifecycleMsg(ComponentLifecycleMsg msg) {
        TbActorRef target = null;
        if (TenantId.SYS_TENANT_ID.equals(msg.getTenantId())) {
            // 处理系统租户的消息
            if (!EntityType.TENANT_PROFILE.equals(msg.getEntityId().getEntityType())) {
                log.warn("Message has system tenant id: {}", msg);
            }
        } else {
            // 处理普通租户的消息
            if (EntityType.TENANT.equals(msg.getEntityId().getEntityType())) {
                TenantId tenantId = TenantId.fromUUID(msg.getEntityId().getId());
                // 处理租户删除事件
                if (msg.getEvent() == ComponentLifecycleEvent.DELETED) {
                    log.info("[{}] Handling tenant deleted notification: {}", msg.getTenantId(), msg);
                    deletedTenants.add(tenantId);
                    ctx.stop(new TbEntityActorId(tenantId));
                    return;
                }
            }
            // 获取或创建目标租户Actor
            target = getOrCreateTenantActor(msg.getTenantId()).orElseGet(() -> {
                log.debug("Ignoring component lifecycle msg for tenant {} because it is not managed by this service", msg.getTenantId());
                return null;
            });
        }
        // 发送消息到目标Actor（高优先级）
        if (target != null) {
            target.tellWithHighPriority(msg);
        } else {
            log.debug("[{}] Invalid component lifecycle msg: {}", msg.getTenantId(), msg);
        }
    }

    /**
     * 处理计算字段系统消息
     */
    private void onToCalculatedFieldSystemActorMsg(ToCalculatedFieldSystemMsg msg, boolean priority) {
        getOrCreateTenantActor(msg.getTenantId()).ifPresentOrElse(tenantActor -> {
            // 根据优先级决定发送方式
            if (priority) {
                tenantActor.tellWithHighPriority(msg);
            } else {
                tenantActor.tell(msg);
            }
        }, () -> {
            // 如果租户Actor不存在，直接回调成功
            msg.getCallback().onSuccess();
        });
    }


    /**
     * 处理设备Actor消息
     * 包括设备属性更新、RPC请求/响应等
     */
    private void onToDeviceActorMsg(TenantAwareMsg msg, boolean priority) {
        getOrCreateTenantActor(msg.getTenantId()).ifPresentOrElse(tenantActor -> {
            // 根据优先级决定发送方式
            if (priority) {
                tenantActor.tellWithHighPriority(msg);
            } else {
                tenantActor.tell(msg);
            }
        }, () -> {
            // 对于传输消息包装器，如果租户Actor不存在，直接回调成功
            if (msg instanceof TransportToDeviceActorMsgWrapper) {
                ((TransportToDeviceActorMsgWrapper) msg).getCallback().onSuccess();
            }
        });
    }

    private Optional<TbActorRef> getOrCreateTenantActor(TenantId tenantId) {
        if (deletedTenants.contains(tenantId)) {
            return Optional.empty();
        }
        return Optional.ofNullable(ctx.getOrCreateChildActor(new TbEntityActorId(tenantId),
                () -> DefaultActorService.TENANT_DISPATCHER_NAME,
                () -> new TenantActor.ActorCreator(systemContext, tenantId),
                () -> true));
    }

    @Override
    public ProcessFailureStrategy onProcessFailure(TbActorMsg msg, Throwable t) {
        log.error("Failed to process msg: {}", msg, t);
        return doProcessFailure(t);
    }

    public static class ActorCreator extends ContextBasedCreator {

        public ActorCreator(ActorSystemContext context) {
            super(context);
        }

        @Override
        public TbActorId createActorId() {
            return new TbEntityActorId(TenantId.SYS_TENANT_ID);
        }

        @Override
        public TbActor createActor() {
            return new AppActor(context);
        }
    }

}
