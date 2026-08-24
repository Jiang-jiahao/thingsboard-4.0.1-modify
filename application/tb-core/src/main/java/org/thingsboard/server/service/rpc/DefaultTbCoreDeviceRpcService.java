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
package org.thingsboard.server.service.rpc;

import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.common.util.ThingsBoardExecutors;
import org.thingsboard.server.actors.ActorSystemContext;
import org.thingsboard.server.cluster.TbClusterService;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.msg.TbMsgType;
import org.thingsboard.server.common.data.rpc.RpcError;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.TbMsgDataType;
import org.thingsboard.server.common.msg.TbMsgMetaData;
import org.thingsboard.server.common.msg.rpc.FromDeviceRpcResponse;
import org.thingsboard.server.common.msg.rpc.RemoveRpcActorMsg;
import org.thingsboard.server.common.msg.rpc.ToDeviceRpcRequest;
import org.thingsboard.server.common.msg.rpc.ToDeviceRpcRequestActorMsg;
import org.thingsboard.server.dao.device.DeviceService;
import org.thingsboard.server.queue.discovery.TbServiceInfoProvider;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.security.model.SecurityUser;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Core 侧设备 RPC 编排服务。
 * <p>
 * REST 发起的服务端 RPC 先入规则引擎（{@code RPC_CALL_FROM_SERVER_TO_DEVICE}），
 * 规则引擎再把请求转到负责该设备的 Core 节点上的 Device Actor；响应经通知队列回到本机回调。
 * 本机维护两张请求映射：REST→规则引擎、规则引擎→设备 Actor，并按过期时间调度超时。
 *
 * @see TbCoreDeviceRpcService
 */
@Service
@Slf4j
@TbCoreComponent
public class DefaultTbCoreDeviceRpcService implements TbCoreDeviceRpcService {

    private final DeviceService deviceService;
    private final TbClusterService clusterService;
    private final TbServiceInfoProvider serviceInfoProvider;
    private final ActorSystemContext actorContext;

    private final ConcurrentMap<UUID, Consumer<FromDeviceRpcResponse>> localToRuleEngineRpcRequests = new ConcurrentHashMap<>();
    private final ConcurrentMap<UUID, ToDeviceRpcRequestActorMsg> localToDeviceRpcRequests = new ConcurrentHashMap<>();

    private Optional<TbRuleEngineDeviceRpcService> tbRuleEngineRpcService;
    private ScheduledExecutorService scheduler;
    private String serviceId;

    public DefaultTbCoreDeviceRpcService(DeviceService deviceService, TbClusterService clusterService, TbServiceInfoProvider serviceInfoProvider,
                                         ActorSystemContext actorContext) {
        this.deviceService = deviceService;
        this.clusterService = clusterService;
        this.serviceInfoProvider = serviceInfoProvider;
        this.actorContext = actorContext;
    }

    /**
     * 可选注入规则引擎侧 RPC 服务（monolith 同进程时本地回传响应）。
     */
    @Autowired(required = false)
    public void setTbRuleEngineRpcService(Optional<TbRuleEngineDeviceRpcService> tbRuleEngineRpcService) {
        this.tbRuleEngineRpcService = tbRuleEngineRpcService;
    }

    /**
     * 启动 RPC 超时调度线程并缓存本节点 serviceId。
     */
    @PostConstruct
    public void initExecutor() {
        scheduler = ThingsBoardExecutors.newSingleThreadScheduledExecutor("tb-core-rpc-scheduler");
        serviceId = serviceInfoProvider.getServiceId();
    }

    /**
     * 关闭超时调度线程。
     */
    @PreDestroy
    public void shutdownExecutor() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    /**
     * 处理 REST API 发起的设备 RPC：登记回调、推入规则引擎并调度超时。
     */
    @Override
    public void processRestApiRpcRequest(ToDeviceRpcRequest request, Consumer<FromDeviceRpcResponse> responseConsumer, SecurityUser currentUser) {
        log.trace("[{}][{}] Processing REST API call to rule engine [{}]", request.getTenantId(), request.getId(), request.getDeviceId());
        UUID requestId = request.getId();
        localToRuleEngineRpcRequests.put(requestId, responseConsumer);
        sendRpcRequestToRuleEngine(request, currentUser);
        scheduleToRuleEngineTimeout(request, requestId);
    }

    /**
     * 规则引擎返回的 RPC 响应：匹配本机等待的 REST 回调。
     */
    @Override
    public void processRpcResponseFromRuleEngine(FromDeviceRpcResponse response) {
        log.trace("[{}] Received response to server-side RPC request from rule engine: [{}]", response.getId(), response);
        UUID requestId = response.getId();
        Consumer<FromDeviceRpcResponse> consumer = localToRuleEngineRpcRequests.remove(requestId);
        if (consumer != null) {
            consumer.accept(response);
        } else {
            log.trace("[{}] Unknown or stale rpc response received [{}]", requestId, response);
        }
    }

    /**
     * 将规则引擎转发来的 RPC 交给本机 Device Actor，并登记待设备响应。
     */
    @Override
    public void forwardRpcRequestToDeviceActor(ToDeviceRpcRequestActorMsg rpcMsg) {
        ToDeviceRpcRequest request = rpcMsg.getMsg();
        log.trace("[{}][{}] Processing local rpc call to device actor [{}]", request.getTenantId(), request.getId(), request.getDeviceId());
        UUID requestId = request.getId();
        localToDeviceRpcRequests.put(requestId, rpcMsg);
        actorContext.tellWithHighPriority(rpcMsg);
        scheduleToDeviceTimeout(request, requestId);
    }

    /**
     * Device Actor 收到设备应答后，回传给发起请求的规则引擎节点。
     */
    @Override
    public void processRpcResponseFromDeviceActor(FromDeviceRpcResponse response) {
        log.trace("[{}] Received response to server-side RPC request from device actor.", response.getId());
        UUID requestId = response.getId();
        ToDeviceRpcRequestActorMsg request = localToDeviceRpcRequests.remove(requestId);
        if (request != null) {
            sendRpcResponseToTbRuleEngine(request.getServiceId(), response);
        } else {
            log.trace("[{}] Unknown or stale rpc response received [{}]", requestId, response);
        }
    }

    /**
     * 通知 Device Actor 移除尚未完成的持久化 RPC。
     */
    @Override
    public void processRemoveRpc(RemoveRpcActorMsg removeRpcMsg) {
        log.trace("[{}][{}] Processing remove RPC [{}]", removeRpcMsg.getTenantId(), removeRpcMsg.getRequestId(), removeRpcMsg.getDeviceId());
        actorContext.tellWithHighPriority(removeRpcMsg);
    }

    private void sendRpcResponseToTbRuleEngine(String originServiceId, FromDeviceRpcResponse response) {
        if (serviceId.equals(originServiceId)) {
            if (tbRuleEngineRpcService.isPresent()) {
                tbRuleEngineRpcService.get().processRpcResponseFromDevice(response);
            } else {
                log.warn("Failed to find tbCoreRpcService for local service. Possible duplication of serviceIds.");
            }
        } else {
            clusterService.pushNotificationToRuleEngine(originServiceId, response, null);
        }
    }

    private void sendRpcRequestToRuleEngine(ToDeviceRpcRequest msg, SecurityUser currentUser) {
        ObjectNode entityNode = JacksonUtil.newObjectNode();
        TbMsgMetaData metaData = new TbMsgMetaData();
        metaData.putValue("requestUUID", msg.getId().toString());
        metaData.putValue("originServiceId", serviceId);
        metaData.putValue("expirationTime", Long.toString(msg.getExpirationTime()));
        metaData.putValue("oneway", Boolean.toString(msg.isOneway()));
        metaData.putValue(DataConstants.PERSISTENT, Boolean.toString(msg.isPersisted()));

        if (msg.getRetries() != null) {
            metaData.putValue(DataConstants.RETRIES, msg.getRetries().toString());
        }


        Device device = deviceService.findDeviceById(msg.getTenantId(), msg.getDeviceId());
        if (device != null) {
            metaData.putValue("deviceName", device.getName());
            metaData.putValue("deviceType", device.getType());
        }

        entityNode.put("method", msg.getBody().getMethod());
        entityNode.put("params", msg.getBody().getParams());

        entityNode.put(DataConstants.ADDITIONAL_INFO, msg.getAdditionalInfo());

        try {
            TbMsg tbMsg = TbMsg.newMsg()
                    .type(TbMsgType.RPC_CALL_FROM_SERVER_TO_DEVICE)
                    .originator(msg.getDeviceId())
                    .customerId(Optional.ofNullable(currentUser).map(User::getCustomerId).orElse(null))
                    .copyMetaData(metaData)
                    .dataType(TbMsgDataType.JSON)
                    .data(JacksonUtil.toString(entityNode))
                    .build();
            clusterService.pushMsgToRuleEngine(msg.getTenantId(), msg.getDeviceId(), tbMsg, null);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

    private void scheduleToRuleEngineTimeout(ToDeviceRpcRequest request, UUID requestId) {
        long timeout = Math.max(0, request.getExpirationTime() - System.currentTimeMillis()) + TimeUnit.SECONDS.toMillis(1);
        log.trace("[{}] processing to rule engine request.", requestId);
        scheduler.schedule(() -> {
            log.trace("[{}] timeout for processing to rule engine request.", requestId);
            Consumer<FromDeviceRpcResponse> consumer = localToRuleEngineRpcRequests.remove(requestId);
            if (consumer != null) {
                consumer.accept(new FromDeviceRpcResponse(requestId, null, RpcError.TIMEOUT));
            }
        }, timeout, TimeUnit.MILLISECONDS);
    }

    private void scheduleToDeviceTimeout(ToDeviceRpcRequest request, UUID requestId) {
        long timeout = Math.max(0, request.getExpirationTime() - System.currentTimeMillis()) + TimeUnit.SECONDS.toMillis(1);
        log.trace("[{}] processing to device request.", requestId);
        scheduler.schedule(() -> {
            log.trace("[{}] timeout for to device request.", requestId);
            localToDeviceRpcRequests.remove(requestId);
        }, timeout, TimeUnit.MILLISECONDS);
    }

}
