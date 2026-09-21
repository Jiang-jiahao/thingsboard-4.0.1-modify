package com.jnks.iot.server.service.rpc;

import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.common.util.JnksIotExecutors;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.cluster.JnksIotClusterService;
import com.jnks.iot.server.common.data.DataConstants;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.rpc.RpcError;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgDataType;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;
import com.jnks.iot.server.common.msg.rpc.FromDeviceRpcResponse;
import com.jnks.iot.server.common.msg.rpc.RemoveRpcActorMsg;
import com.jnks.iot.server.common.msg.rpc.ToDeviceRpcRequest;
import com.jnks.iot.server.common.msg.rpc.ToDeviceRpcRequestActorMsg;
import com.jnks.iot.server.dao.device.DeviceService;
import com.jnks.iot.server.queue.discovery.JnksIotServiceInfoProvider;
import com.jnks.iot.server.service.security.model.SecurityUser;

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
 * @see JnksIotCoreDeviceRpcService
 */
@Service
@Slf4j
public class DefaultJnksIotCoreDeviceRpcService implements JnksIotCoreDeviceRpcService {

    private final DeviceService deviceService;
    private final JnksIotClusterService clusterService;
    private final JnksIotServiceInfoProvider serviceInfoProvider;
    private final ActorSystemContext actorContext;

    private final ConcurrentMap<UUID, Consumer<FromDeviceRpcResponse>> localToRuleEngineRpcRequests = new ConcurrentHashMap<>();
    private final ConcurrentMap<UUID, ToDeviceRpcRequestActorMsg> localToDeviceRpcRequests = new ConcurrentHashMap<>();

    private Optional<JnksIotRuleEngineDeviceRpcService> jnksIotRuleEngineRpcService;
    private ScheduledExecutorService scheduler;
    private String serviceId;

    public DefaultJnksIotCoreDeviceRpcService(DeviceService deviceService, JnksIotClusterService clusterService, JnksIotServiceInfoProvider serviceInfoProvider,
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
    public void setJnksIotRuleEngineRpcService(Optional<JnksIotRuleEngineDeviceRpcService> jnksIotRuleEngineRpcService) {
        this.jnksIotRuleEngineRpcService = jnksIotRuleEngineRpcService;
    }

    /**
     * 启动 RPC 超时调度线程并缓存本节点 serviceId。
     */
    @PostConstruct
    public void initExecutor() {
        scheduler = JnksIotExecutors.newSingleThreadScheduledExecutor("jnks-iot-core-rpc-scheduler");
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
            sendRpcResponseToJnksIotRuleEngine(request.getServiceId(), response);
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

    private void sendRpcResponseToJnksIotRuleEngine(String originServiceId, FromDeviceRpcResponse response) {
        if (serviceId.equals(originServiceId)) {
            if (jnksIotRuleEngineRpcService.isPresent()) {
                jnksIotRuleEngineRpcService.get().processRpcResponseFromDevice(response);
            } else {
                log.warn("Failed to find jnksIotCoreRpcService for local service. Possible duplication of serviceIds.");
            }
        } else {
            clusterService.pushNotificationToRuleEngine(originServiceId, response, null);
        }
    }

    private void sendRpcRequestToRuleEngine(ToDeviceRpcRequest msg, SecurityUser currentUser) {
        ObjectNode entityNode = JacksonUtil.newObjectNode();
        JnksIotMsgMetaData metaData = new JnksIotMsgMetaData();
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
            JnksIotMsg jnksIotMsg = JnksIotMsg.newMsg()
                    .type(JnksIotMsgType.RPC_CALL_FROM_SERVER_TO_DEVICE)
                    .originator(msg.getDeviceId())
                    .customerId(Optional.ofNullable(currentUser).map(User::getCustomerId).orElse(null))
                    .copyMetaData(metaData)
                    .dataType(JnksIotMsgDataType.JSON)
                    .data(JacksonUtil.toString(entityNode))
                    .build();
            clusterService.pushMsgToRuleEngine(msg.getTenantId(), msg.getDeviceId(), jnksIotMsg, null);
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
