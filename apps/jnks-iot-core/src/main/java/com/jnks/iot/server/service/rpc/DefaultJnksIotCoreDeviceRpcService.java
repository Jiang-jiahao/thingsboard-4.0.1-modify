package com.jnks.iot.server.service.rpc;

import com.fasterxml.jackson.databind.JsonNode;
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
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileRpcBindingType;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileRpcMethod;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.rpc.RpcError;
import com.jnks.iot.server.common.data.rpc.ToDeviceRpcRequestBody;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgDataType;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;
import com.jnks.iot.server.common.msg.rpc.FromDeviceRpcResponse;
import com.jnks.iot.server.common.msg.rpc.RemoveRpcActorMsg;
import com.jnks.iot.server.common.msg.rpc.ToDeviceRpcRequest;
import com.jnks.iot.server.common.msg.rpc.ToDeviceRpcRequestActorMsg;
import com.jnks.iot.server.dao.device.DeviceProfileService;
import com.jnks.iot.server.dao.device.DeviceService;
import com.jnks.iot.server.queue.discovery.JnksIotServiceInfoProvider;
import com.jnks.iot.server.service.protocoltemplate.ProtocolTemplateRpcFramer;
import com.jnks.iot.server.service.security.model.SecurityUser;

import java.util.List;
import java.util.Map;
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
    private final DeviceProfileService deviceProfileService;
    private final JnksIotClusterService clusterService;
    private final JnksIotServiceInfoProvider serviceInfoProvider;
    private final ActorSystemContext actorContext;
    private final ProtocolTemplateRpcFramer protocolTemplateRpcFramer;

    private final ConcurrentMap<UUID, Consumer<FromDeviceRpcResponse>> localToRuleEngineRpcRequests = new ConcurrentHashMap<>();
    private final ConcurrentMap<UUID, ToDeviceRpcRequestActorMsg> localToDeviceRpcRequests = new ConcurrentHashMap<>();

    private Optional<JnksIotRuleEngineDeviceRpcService> jnksIotRuleEngineRpcService;
    private ScheduledExecutorService scheduler;
    private String serviceId;

    public DefaultJnksIotCoreDeviceRpcService(DeviceService deviceService, DeviceProfileService deviceProfileService,
                                         JnksIotClusterService clusterService, JnksIotServiceInfoProvider serviceInfoProvider,
                                         ActorSystemContext actorContext, ProtocolTemplateRpcFramer protocolTemplateRpcFramer) {
        this.deviceService = deviceService;
        this.deviceProfileService = deviceProfileService;
        this.clusterService = clusterService;
        this.serviceInfoProvider = serviceInfoProvider;
        this.actorContext = actorContext;
        this.protocolTemplateRpcFramer = protocolTemplateRpcFramer;
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
        Device device = deviceService.findDeviceById(request.getTenantId(), request.getDeviceId());
        ToDeviceRpcRequest effective;
        try {
            effective = frameTemplateRpcIfNeeded(request, device);
        } catch (IllegalArgumentException e) {
            log.warn("[{}][{}] Failed to build protocol template frame for device [{}]: {}",
                    request.getTenantId(), requestId, request.getDeviceId(), e.getMessage());
            responseConsumer.accept(new FromDeviceRpcResponse(requestId, null, RpcError.INTERNAL));
            return;
        }
        localToRuleEngineRpcRequests.put(requestId, responseConsumer);
        sendRpcRequestToRuleEngine(effective, currentUser, device);
        scheduleToRuleEngineTimeout(effective, requestId);
    }

    /**
     * 绑定协议模板的 RPC，管理端由前端组帧、定时任务由调度侧组帧；REST/脚本直调没有前两者，
     * 在这里补上，否则传输层拿不到 {@code params.hex} 会把 JSON 信封当负载发出去。
     */
    private ToDeviceRpcRequest frameTemplateRpcIfNeeded(ToDeviceRpcRequest request, Device device) {
        if (device == null || device.getDeviceProfileId() == null || request.getBody() == null) {
            return request;
        }
        DeviceProfile profile = deviceProfileService.findDeviceProfileById(request.getTenantId(), device.getDeviceProfileId());
        if (profile == null) {
            return request;
        }
        DeviceProfileRpcMethod method = findRpcMethod(profile, request.getBody().getMethod());
        if (method == null) {
            return request;
        }
        String params = request.getBody().getParams();
        // 原生 RPC：调用方可以只传方法名，参数由档案方法的参数模板 + 设备固定参数补齐，
        // 与前端 RPC 面板行为一致；调用方显式传入的值优先。
        if (method.getBindingType() == DeviceProfileRpcBindingType.NATIVE) {
            String merged = mergeNativeRpcParams(device, method, params);
            if (merged != null && !merged.equals(params)) {
                return new ToDeviceRpcRequest(
                        request.getId(),
                        request.getTenantId(),
                        request.getDeviceId(),
                        request.isOneway(),
                        request.getExpirationTime(),
                        new ToDeviceRpcRequestBody(request.getBody().getMethod(), merged),
                        request.isPersisted(),
                        request.getRetries(),
                        request.getAdditionalInfo());
            }
            return request;
        }
        // 自定义负载：params 本身就是下发负载。方法带 JSON 形式的默认负载时，
        // 按对象合并「默认负载 → 设备固定参数 → 调用方参数」；纯文本负载不合并，原样透传。
        if (method.getBindingType() == DeviceProfileRpcBindingType.CUSTOM_PAYLOAD) {
            String merged = mergeCustomPayloadParams(device, method, params);
            if (merged != null && !merged.equals(params)) {
                return new ToDeviceRpcRequest(
                        request.getId(),
                        request.getTenantId(),
                        request.getDeviceId(),
                        request.isOneway(),
                        request.getExpirationTime(),
                        new ToDeviceRpcRequestBody(request.getBody().getMethod(), merged),
                        request.isPersisted(),
                        request.getRetries(),
                        request.getAdditionalInfo());
            }
            return request;
        }
        // 协议模板 RPC：同样补齐设备固定参数，调用方显式传入的值优先；
        // 调用方已带 hex 时视为自行组帧，不再合并。
        String templateParams = hasHexParam(params) ? params : mergeRpcParams(device, method, params, false);
        String framed = protocolTemplateRpcFramer.frameIfNeeded(request.getTenantId(), profile, method, templateParams);
        if (framed == null ? templateParams == null : framed.equals(templateParams)) {
            return request;
        }
        return new ToDeviceRpcRequest(
                request.getId(),
                request.getTenantId(),
                request.getDeviceId(),
                request.isOneway(),
                request.getExpirationTime(),
                new ToDeviceRpcRequestBody(method.getId(), framed),
                request.isPersisted(),
                request.getRetries(),
                request.getAdditionalInfo());
    }

    /**
     * 合并原生 RPC 参数：方法参数模板 → 设备固定参数 → 调用方参数（优先级递增）。
     * 调用方未传参数时返回合并结果；三者都没有时原样返回。
     */
    static String mergeNativeRpcParams(Device device, DeviceProfileRpcMethod method, String callerParams) {
        return mergeRpcParams(device, method, callerParams, true);
    }

    /**
     * 自定义负载 RPC：params 即负载。只有默认负载是 JSON 对象时才按键合并，
     * 默认负载或调用方负载是纯文本时原样透传。
     */
    static String mergeCustomPayloadParams(Device device, DeviceProfileRpcMethod method, String callerParams) {
        String template = method.getParamsTemplateJson();
        if (!isJsonObject(template)) {
            // 纯文本默认负载无法按键合并：调用方没传时用默认文本，传了就用调用方的
            boolean callerBlank = callerParams == null || callerParams.isBlank();
            return callerBlank && template != null && !template.isBlank() ? template : callerParams;
        }
        return mergeRpcParams(device, method, callerParams, true);
    }

    /**
     * 合并 RPC 参数，与前端 RPC 面板一致：设备固定参数 → 调用方参数（调用方优先）。
     * 原生绑定额外把档案方法上的 {@code paramsTemplateJson} 作为最低优先级；
     * 协议模板绑定由模板命令本身提供固定字段，不使用该模板。
     */
    static String mergeRpcParams(Device device, DeviceProfileRpcMethod method, String callerParams,
                                 boolean includeMethodTemplate) {
        // 非 JSON 对象（字符串、数组等）无法按键合并，保持原样透传，避免把调用方负载丢掉
        if (callerParams != null && !callerParams.isBlank() && !isJsonObject(callerParams)) {
            return callerParams;
        }
        ObjectNode merged = JacksonUtil.newObjectNode();
        boolean any = includeMethodTemplate && mergeJsonObject(merged, method.getParamsTemplateJson());
        Map<String, Object> defaults = resolveDeviceRpcDefaults(device, method.getId());
        if (!defaults.isEmpty()) {
            any |= mergeJsonObject(merged, JacksonUtil.toString(defaults));
        }
        any |= mergeJsonObject(merged, callerParams);
        return any ? JacksonUtil.toString(merged) : callerParams;
    }

    /** 与前端 {@code resolveMethodRpcDefaults} 一致：按方法配置优先，全局固定参数只在没有任何按方法配置时生效。 */
    private static Map<String, Object> resolveDeviceRpcDefaults(Device device, String methodId) {
        if (device == null || device.getDeviceData() == null) {
            return Map.of();
        }
        Map<String, Map<String, Object>> byMethod = device.getDeviceData().getRpcParamDefaultsByMethod();
        Map<String, Object> perMethod = byMethod != null ? byMethod.get(methodId) : null;
        if (perMethod != null && !perMethod.isEmpty()) {
            return perMethod;
        }
        if (byMethod == null || byMethod.isEmpty()) {
            Map<String, Object> legacy = device.getDeviceData().getRpcParamDefaults();
            if (legacy != null && !legacy.isEmpty()) {
                return legacy;
            }
        }
        return Map.of();
    }

    private static boolean hasHexParam(String params) {
        if (params == null || params.isBlank()) {
            return false;
        }
        try {
            JsonNode node = JacksonUtil.toJsonNode(params);
            return node != null && node.isObject() && node.has("hex");
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean isJsonObject(String json) {
        try {
            JsonNode node = JacksonUtil.toJsonNode(json);
            return node != null && node.isObject();
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean mergeJsonObject(ObjectNode target, String json) {
        if (json == null || json.isBlank()) {
            return false;
        }
        try {
            JsonNode node = JacksonUtil.toJsonNode(json);
            if (node != null && node.isObject()) {
                node.fields().forEachRemaining(e -> target.set(e.getKey(), e.getValue()));
                return true;
            }
        } catch (Exception e) {
            log.debug("Skip RPC params merge (not a JSON object): {}", e.getMessage());
        }
        return false;
    }

    private static DeviceProfileRpcMethod findRpcMethod(DeviceProfile profile, String methodId) {
        if (methodId == null || profile.getProfileData() == null) {
            return null;
        }
        List<DeviceProfileRpcMethod> methods = profile.getProfileData().getRpcMethods();
        if (methods == null) {
            return null;
        }
        for (DeviceProfileRpcMethod m : methods) {
            if (m != null && methodId.equals(m.getId())) {
                return m;
            }
        }
        return null;
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

    private void sendRpcRequestToRuleEngine(ToDeviceRpcRequest msg, SecurityUser currentUser, Device device) {
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
