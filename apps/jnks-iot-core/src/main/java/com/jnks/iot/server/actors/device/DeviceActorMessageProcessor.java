package com.jnks.iot.server.actors.device;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.common.util.LinkedHashMapRemoveEldest;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.JnksIotActorCtx;
import com.jnks.iot.server.actors.shared.AbstractContextAwareMsgProcessor;
import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.DataConstants;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.RpcId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.kv.AttributeKey;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.page.SortOrder;
import com.jnks.iot.server.common.data.rpc.Rpc;
import com.jnks.iot.server.common.data.rpc.RpcError;
import com.jnks.iot.server.common.data.rpc.RpcStatus;
import com.jnks.iot.server.common.data.rpc.ToDeviceRpcRequestBody;
import com.jnks.iot.server.common.data.security.DeviceCredentials;
import com.jnks.iot.server.common.data.security.DeviceCredentialsType;
import com.jnks.iot.server.common.msg.JnksIotActorMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.common.msg.queue.JnksIotCallback;
import com.jnks.iot.server.common.msg.rpc.FromDeviceRpcResponse;
import com.jnks.iot.server.common.msg.rpc.FromDeviceRpcResponseActorMsg;
import com.jnks.iot.server.common.msg.rpc.RemoveRpcActorMsg;
import com.jnks.iot.server.common.msg.rpc.ToDeviceRpcRequest;
import com.jnks.iot.server.common.msg.rpc.ToDeviceRpcRequestActorMsg;
import com.jnks.iot.server.common.msg.rule.engine.DeviceAttributesEventNotificationMsg;
import com.jnks.iot.server.common.msg.rule.engine.DeviceCredentialsUpdateNotificationMsg;
import com.jnks.iot.server.common.msg.rule.engine.DeviceNameOrTypeUpdateMsg;
import com.jnks.iot.server.common.msg.timeout.DeviceActorServerSideRpcTimeoutMsg;
import com.jnks.iot.server.common.util.KvProtoUtil;
import com.jnks.iot.server.gen.transport.TransportProtos.AttributeUpdateNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ClaimDeviceMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.DeviceSessionsCacheEntry;
import com.jnks.iot.server.gen.transport.TransportProtos.GetAttributeRequestMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.GetAttributeResponseMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.SessionCloseNotificationProto;
import com.jnks.iot.server.gen.transport.TransportProtos.SessionCloseReason;
import com.jnks.iot.server.gen.transport.TransportProtos.SessionEvent;
import com.jnks.iot.server.gen.transport.TransportProtos.SessionEventMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.SessionInfoProto;
import com.jnks.iot.server.gen.transport.TransportProtos.SessionSubscriptionInfoProto;
import com.jnks.iot.server.gen.transport.TransportProtos.SessionType;
import com.jnks.iot.server.gen.transport.TransportProtos.SubscribeToAttributeUpdatesMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.SubscribeToRPCMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.SubscriptionInfoProto;
import com.jnks.iot.server.gen.transport.TransportProtos.ToDeviceRpcRequestMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToDeviceRpcResponseMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToDeviceRpcResponseStatusMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToTransportMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToTransportUpdateCredentialsProto;
import com.jnks.iot.server.gen.transport.TransportProtos.TransportToDeviceActorMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.TsKvProto;
import com.jnks.iot.server.gen.transport.TransportProtos.UplinkNotificationMsg;
import com.jnks.iot.server.service.rpc.RpcSubmitStrategy;
import com.jnks.iot.server.service.transport.msg.TransportToDeviceActorMsgWrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;


/**
 * @author Andrew Shvayka
 */
@Slf4j
public class DeviceActorMessageProcessor extends AbstractContextAwareMsgProcessor {

    static final String SESSION_TIMEOUT_MESSAGE = "session timeout!";
    final TenantId tenantId;
    final DeviceId deviceId;
    final LinkedHashMapRemoveEldest<UUID, SessionInfoMetaData> sessions;
    final Map<UUID, SessionInfo> attributeSubscriptions;
    final Map<UUID, SessionInfo> rpcSubscriptions;
    private final Map<Integer, ToDeviceRpcRequestMetadata> toDeviceRpcPendingMap;
    private final boolean rpcSequential;
    private final RpcSubmitStrategy rpcSubmitStrategy;
    private final ScheduledExecutorService scheduler;
    private final boolean closeTransportSessionOnRpcDeliveryTimeout;

    private int rpcSeq = 0;
    private String deviceName;
    private String deviceType;
    private JnksIotMsgMetaData defaultMetaData;
    private ScheduledFuture<?> awaitRpcResponseFuture;

    DeviceActorMessageProcessor(ActorSystemContext systemContext, TenantId tenantId, DeviceId deviceId) {
        super(systemContext);
        this.tenantId = tenantId;
        this.deviceId = deviceId;
        this.rpcSubmitStrategy = RpcSubmitStrategy.parse(systemContext.getRpcSubmitStrategy());
        this.closeTransportSessionOnRpcDeliveryTimeout = systemContext.isCloseTransportSessionOnRpcDeliveryTimeout();
        this.rpcSequential = !rpcSubmitStrategy.equals(RpcSubmitStrategy.BURST);
        this.attributeSubscriptions = new HashMap<>();
        this.rpcSubscriptions = new HashMap<>();
        this.toDeviceRpcPendingMap = new LinkedHashMap<>();
        this.sessions = new LinkedHashMapRemoveEldest<>(systemContext.getMaxConcurrentSessionsPerDevice(), this::notifyTransportAboutClosedSessionMaxSessionsLimit);
        this.scheduler = systemContext.getScheduler();
        if (initAttributes()) {
            restoreSessions();
        }
    }

    boolean initAttributes() {
        Device device = systemContext.getDeviceService().findDeviceById(tenantId, deviceId);
        if (device != null) {
            this.deviceName = device.getName();
            this.deviceType = device.getType();
            this.defaultMetaData = new JnksIotMsgMetaData();
            this.defaultMetaData.putValue("deviceName", deviceName);
            this.defaultMetaData.putValue("deviceType", deviceType);
            return true;
        } else {
            return false;
        }
    }

    void processRpcRequest(JnksIotActorCtx context, ToDeviceRpcRequestActorMsg msg) {
        ToDeviceRpcRequest request = msg.getMsg();
        UUID rpcId = request.getId();
        log.debug("[{}][{}] Received RPC request to process ...", deviceId, rpcId);
        ToDeviceRpcRequestMsg rpcRequest = createToDeviceRpcRequestMsg(request);

        long timeout = request.getExpirationTime() - System.currentTimeMillis();
        boolean persisted = request.isPersisted();

        if (timeout <= 0) {
            log.debug("[{}][{}] Ignoring message due to exp time reached, {}", deviceId, rpcId, request.getExpirationTime());
            if (persisted) {
                createRpc(request, RpcStatus.EXPIRED);
            }
            return;
        } else if (persisted) {
            createRpc(request, RpcStatus.QUEUED);
        }

        boolean sent = false;
        int requestId = rpcRequest.getRequestId();
        if (isSendNewRpcAvailable()) {
            Map<UUID, SessionInfo> targets = rpcDispatchTargets();
            sent = !targets.isEmpty();
            Set<UUID> syncSessionSet = new HashSet<>();
            targets.forEach((sessionId, sessionInfo) -> {
                log.debug("[{}][{}][{}][{}] send RPC request to transport ...", deviceId, sessionId, rpcId, requestId);
                sendToTransport(rpcRequest, sessionId, sessionInfo.getNodeId());
                if (SessionType.SYNC == sessionInfo.getType()) {
                    syncSessionSet.add(sessionId);
                }
            });
            log.trace("Rpc syncSessionSet [{}] subscription after sent [{}]", syncSessionSet, rpcSubscriptions);
            syncSessionSet.forEach(rpcSubscriptions::remove);
        }

        if (persisted) {
            ObjectNode response = JacksonUtil.newObjectNode();
            response.put("rpcId", rpcId.toString());
            systemContext.getJnksIotCoreDeviceRpcService().processRpcResponseFromDeviceActor(new FromDeviceRpcResponse(rpcId, JacksonUtil.toString(response), null));
        }

        if (!persisted && request.isOneway() && sent) {
            log.debug("[{}] RPC command response sent [{}][{}]!", deviceId, rpcId, requestId);
            systemContext.getJnksIotCoreDeviceRpcService().processRpcResponseFromDeviceActor(new FromDeviceRpcResponse(rpcId, null, null));
        } else {
            registerPendingRpcRequest(context, msg, sent, rpcRequest, timeout);
        }
        String rpcSent = sent ? "sent!" : "NOT sent!";
        log.debug("[{}][{}][{}] RPC request is {}", deviceId, rpcId, requestId, rpcSent);
    }

    private boolean isSendNewRpcAvailable() {
        return switch (rpcSubmitStrategy) {
            case SEQUENTIAL_ON_ACK_FROM_DEVICE -> toDeviceRpcPendingMap.values().stream().filter(md -> !md.isDelivered()).findAny().isEmpty();
            case SEQUENTIAL_ON_RESPONSE_FROM_DEVICE -> toDeviceRpcPendingMap.isEmpty();
            default -> true;
        };
    }

    private void createRpc(ToDeviceRpcRequest request, RpcStatus status) {
        Rpc rpc = new Rpc(new RpcId(request.getId()));
        rpc.setCreatedTime(System.currentTimeMillis());
        rpc.setTenantId(tenantId);
        rpc.setDeviceId(deviceId);
        rpc.setExpirationTime(request.getExpirationTime());
        rpc.setRequest(JacksonUtil.valueToTree(request));
        rpc.setStatus(status);
        rpc.setAdditionalInfo(JacksonUtil.toJsonNode(request.getAdditionalInfo()));
        systemContext.getJnksIotRpcService().save(tenantId, rpc);
    }

    private ToDeviceRpcRequestMsg createToDeviceRpcRequestMsg(ToDeviceRpcRequest request) {
        ToDeviceRpcRequestBody body = request.getBody();
        return ToDeviceRpcRequestMsg.newBuilder()
                .setRequestId(rpcSeq++)
                .setMethodName(body.getMethod())
                .setParams(body.getParams())
                .setExpirationTime(request.getExpirationTime())
                .setRequestIdMSB(request.getId().getMostSignificantBits())
                .setRequestIdLSB(request.getId().getLeastSignificantBits())
                .setOneway(request.isOneway())
                .setPersisted(request.isPersisted())
                .build();
    }

    void processRpcResponse(FromDeviceRpcResponseActorMsg responseMsg) {
        log.debug("[{}] Processing RPC command response", deviceId);
        ToDeviceRpcRequestMetadata requestMd = toDeviceRpcPendingMap.remove(responseMsg.getRequestId());
        boolean success = requestMd != null;
        if (success) {
            systemContext.getJnksIotCoreDeviceRpcService().processRpcResponseFromDeviceActor(responseMsg.getMsg());
        } else {
            log.debug("[{}] RPC command response [{}] is stale!", deviceId, responseMsg.getRequestId());
        }
    }

    void processRemoveRpc(RemoveRpcActorMsg msg) {
        UUID rpcId = msg.getRequestId();
        log.debug("[{}][{}] Received remove RPC request ...", deviceId, rpcId);
        Map.Entry<Integer, ToDeviceRpcRequestMetadata> entry = null;
        for (Map.Entry<Integer, ToDeviceRpcRequestMetadata> e : toDeviceRpcPendingMap.entrySet()) {
            if (e.getValue().getMsg().getMsg().getId().equals(rpcId)) {
                entry = e;
                break;
            }
        }

        if (entry != null) {
            Integer requestId = entry.getKey();
            if (entry.getValue().isDelivered()) {
                toDeviceRpcPendingMap.remove(requestId);
                if (rpcSubmitStrategy.equals(RpcSubmitStrategy.SEQUENTIAL_ON_RESPONSE_FROM_DEVICE)) {
                    clearAwaitRpcResponseScheduler();
                    sendNextPendingRequest(rpcId, requestId, "Removed pending RPC!");
                }
            } else {
                Optional<Map.Entry<Integer, ToDeviceRpcRequestMetadata>> firstRpc = getFirstRpc();
                if (firstRpc.isPresent() && requestId.equals(firstRpc.get().getKey())) {
                    toDeviceRpcPendingMap.remove(requestId);
                    sendNextPendingRequest(rpcId, requestId, "Removed pending RPC!");
                } else {
                    toDeviceRpcPendingMap.remove(requestId);
                }
            }
        }
    }

    private void registerPendingRpcRequest(JnksIotActorCtx context, ToDeviceRpcRequestActorMsg msg, boolean sent, ToDeviceRpcRequestMsg rpcRequest, long timeout) {
        int requestId = rpcRequest.getRequestId();
        UUID rpcId = new UUID(rpcRequest.getRequestIdMSB(), rpcRequest.getRequestIdLSB());
        log.debug("[{}][{}][{}] Registering pending RPC request...", deviceId, rpcId, requestId);
        toDeviceRpcPendingMap.put(requestId, new ToDeviceRpcRequestMetadata(msg, sent));
        DeviceActorServerSideRpcTimeoutMsg timeoutMsg = new DeviceActorServerSideRpcTimeoutMsg(requestId, timeout);
        scheduleMsgWithDelay(context, timeoutMsg, timeoutMsg.getTimeout());
    }

    void processServerSideRpcTimeout(DeviceActorServerSideRpcTimeoutMsg msg) {
        Integer requestId = msg.getId();
        var requestMd = toDeviceRpcPendingMap.remove(requestId);
        if (requestMd != null) {
            var toDeviceRpcRequest = requestMd.getMsg().getMsg();
            UUID rpcId = toDeviceRpcRequest.getId();
            log.debug("[{}][{}][{}] RPC request timeout detected!", deviceId, rpcId, requestId);
            if (toDeviceRpcRequest.isPersisted()) {
                systemContext.getJnksIotRpcService().save(tenantId, new RpcId(rpcId), RpcStatus.EXPIRED, null);
            }
            systemContext.getJnksIotCoreDeviceRpcService().processRpcResponseFromDeviceActor(new FromDeviceRpcResponse(rpcId,
                    null, requestMd.isSent() ? RpcError.TIMEOUT : RpcError.NO_ACTIVE_CONNECTION));
            if (!requestMd.isDelivered()) {
                sendNextPendingRequest(rpcId, requestId, "Pending RPC timeout detected!");
                return;
            }
            if (rpcSubmitStrategy.equals(RpcSubmitStrategy.SEQUENTIAL_ON_RESPONSE_FROM_DEVICE)) {
                clearAwaitRpcResponseScheduler();
                sendNextPendingRequest(rpcId, requestId, "Pending RPC timeout detected!");
            }
        }
    }

    private void sendPendingRequests(UUID sessionId, String nodeId) {
        SessionType sessionType = getSessionType(sessionId);
        if (!toDeviceRpcPendingMap.isEmpty()) {
            log.debug("[{}] Pushing {} pending RPC messages to session: [{}]", deviceId, sessionId, toDeviceRpcPendingMap.size());
            if (sessionType == SessionType.SYNC) {
                log.debug("[{}] Cleanup sync RPC session [{}]", deviceId, sessionId);
                rpcSubscriptions.remove(sessionId);
            }
        } else {
            log.debug("[{}] No pending RPC messages for session: [{}]", deviceId, sessionId);
        }
        Set<Integer> sentOneWayIds = new HashSet<>();

        if (rpcSequential) {
            getFirstRpc().ifPresent(processPendingRpc(sessionId, nodeId, sentOneWayIds));
        } else if (sessionType == SessionType.ASYNC) {
            toDeviceRpcPendingMap.entrySet().forEach(processPendingRpc(sessionId, nodeId, sentOneWayIds));
        } else {
            toDeviceRpcPendingMap.entrySet().stream().findFirst().ifPresent(processPendingRpc(sessionId, nodeId, sentOneWayIds));
        }

        sentOneWayIds.stream().filter(id -> !toDeviceRpcPendingMap.get(id).getMsg().getMsg().isPersisted()).forEach(toDeviceRpcPendingMap::remove);
    }

    private Optional<Map.Entry<Integer, ToDeviceRpcRequestMetadata>> getFirstRpc() {
        if (rpcSubmitStrategy.equals(RpcSubmitStrategy.SEQUENTIAL_ON_RESPONSE_FROM_DEVICE)) {
            return toDeviceRpcPendingMap.entrySet().stream()
                    .findFirst().filter(entry -> {
                        var md = entry.getValue();
                        if (md.isDelivered()) {
                            if (awaitRpcResponseFuture == null || awaitRpcResponseFuture.isCancelled()) {
                                var toDeviceRpcRequest = md.getMsg().getMsg();
                                awaitRpcResponseFuture = scheduleAwaitRpcResponseFuture(toDeviceRpcRequest.getId(), entry.getKey());
                            }
                            return false;
                        }
                        return true;
                    });
        }
        return toDeviceRpcPendingMap.entrySet().stream().filter(e -> !e.getValue().isDelivered()).findFirst();
    }

    private void sendNextPendingRequest(UUID rpcId, int requestId, String logMessage) {
        log.debug("[{}][{}][{}] {} Going to send next pending request ...", deviceId, rpcId, requestId, logMessage);
        if (rpcSequential) {
            rpcDispatchTargets().forEach((id, s) -> sendPendingRequests(id, s.getNodeId()));
        }
    }

    /**
     * 本次下发要投递的 RPC 订阅。
     * <p>
     * 有的传输层会给"设备还没开口"的设备建**虚拟会话** —— 只发 RPC 订阅、**不发**
     * {@code SESSION_EVENT_MSG_OPEN}（例如 UDP 为被动设备建的出站会话，见
     * {@code UdpOutboundTransportContext}）。这类会话不在 {@link #sessions} 里，
     * 因此不受"每设备最大并发会话数"的淘汰约束；设备随后真开口时，真实会话与虚拟会话
     * 会**同时**留在 {@code rpcSubscriptions} 里，逐个投递就会把同一条 RPC 下发两次
     * （实测：真实会话与出站会话各收到一次）。
     * <p>
     * 所以优先只投给有真实会话的订阅；设备当前没有任何真实会话时才回退到虚拟订阅
     * —— 那时它是唯一的通道（设备一开口就会被上面这条规则让位）。
     * <p>
     * 例外：{@link SessionType#SYNC} 的订阅必须始终保留。SYNC 会话由传输层
     * {@code registerSyncSession} 注册（如 HTTP 长轮询 {@code GET /api/v1/{token}/rpc}），
     * 同样**不发** {@code SESSION_EVENT_MSG_OPEN}，因此也不在 {@link #sessions} 里 ——
     * 但它是一次性的**真实下发目标**，不是本规则要消除的虚拟会话。若把它一并排除，
     * 当该设备同时还有出站/拉取会话时（后者在 {@code sessions} 里），长轮询订阅会被丢弃，
     * NATIVE RPC 就永远到不了设备（HTTP 实测：twoway 504、长轮询 408）。
     * 排除虚拟会话（BUG-3 场景）不会因此回退：虚拟会话经 BUG-4 修复后是 ASYNC。
     */
    private Map<UUID, SessionInfo> rpcDispatchTargets() {
        Map<UUID, SessionInfo> live = new HashMap<>();
        rpcSubscriptions.forEach((sessionId, sessionInfo) -> {
            if (sessions.containsKey(sessionId) || SessionType.SYNC == sessionInfo.getType()) {
                live.put(sessionId, sessionInfo);
            }
        });
        return live.isEmpty() ? rpcSubscriptions : live;
    }

    private Consumer<Map.Entry<Integer, ToDeviceRpcRequestMetadata>> processPendingRpc(UUID sessionId, String nodeId, Set<Integer> sentOneWayIds) {
        return entry -> {
            ToDeviceRpcRequest request = entry.getValue().getMsg().getMsg();
            ToDeviceRpcRequestBody body = request.getBody();
            Integer requestId = entry.getKey();
            UUID rpcId = request.getId();
            if (request.isOneway() && !rpcSequential) {
                sentOneWayIds.add(requestId);
                systemContext.getJnksIotCoreDeviceRpcService().processRpcResponseFromDeviceActor(new FromDeviceRpcResponse(rpcId, null, null));
            }
            ToDeviceRpcRequestMsg rpcRequest = ToDeviceRpcRequestMsg.newBuilder()
                    .setRequestId(requestId)
                    .setMethodName(body.getMethod())
                    .setParams(body.getParams())
                    .setExpirationTime(request.getExpirationTime())
                    .setRequestIdMSB(rpcId.getMostSignificantBits())
                    .setRequestIdLSB(rpcId.getLeastSignificantBits())
                    .setOneway(request.isOneway())
                    .setPersisted(request.isPersisted())
                    .build();
            log.debug("[{}][{}][{}][{}] Send pending RPC request to transport ...", deviceId, sessionId, rpcId, requestId);
            sendToTransport(rpcRequest, sessionId, nodeId);
        };
    }

    void process(TransportToDeviceActorMsgWrapper wrapper) {
        TransportToDeviceActorMsg msg = wrapper.getMsg();
        JnksIotCallback callback = wrapper.getCallback();
        var sessionInfo = msg.getSessionInfo();

        if (msg.hasSessionEvent()) {
            processSessionStateMsgs(sessionInfo, msg.getSessionEvent());
        }
        if (msg.hasSubscribeToAttributes()) {
            processSubscriptionCommands(sessionInfo, msg.getSubscribeToAttributes());
        }
        if (msg.hasSubscribeToRPC()) {
            processSubscriptionCommands(sessionInfo, msg.getSubscribeToRPC());
        }
        if (msg.hasSendPendingRPC()) {
            sendPendingRequests(getSessionId(sessionInfo), sessionInfo.getNodeId());
        }
        if (msg.hasGetAttributes()) {
            handleGetAttributesRequest(sessionInfo, msg.getGetAttributes());
        }
        if (msg.hasToDeviceRPCCallResponse()) {
            processRpcResponses(sessionInfo, msg.getToDeviceRPCCallResponse());
        }
        if (msg.hasSubscriptionInfo()) {
            handleSessionActivity(sessionInfo, msg.getSubscriptionInfo());
        }
        if (msg.hasClaimDevice()) {
            handleClaimDeviceMsg(sessionInfo, msg.getClaimDevice());
        }
        if (msg.hasRpcResponseStatusMsg()) {
            processRpcResponseStatus(sessionInfo, msg.getRpcResponseStatusMsg());
        }
        if (msg.hasUplinkNotificationMsg()) {
            processUplinkNotificationMsg(sessionInfo, msg.getUplinkNotificationMsg());
        }
        callback.onSuccess();
    }

    private void processUplinkNotificationMsg(SessionInfoProto sessionInfo, UplinkNotificationMsg uplinkNotificationMsg) {
        String nodeId = sessionInfo.getNodeId();
        sessions.entrySet().stream()
                .filter(kv -> kv.getValue().getSessionInfo().getNodeId().equals(nodeId) && (kv.getValue().isSubscribedToAttributes() || kv.getValue().isSubscribedToRPC()))
                .forEach(kv -> {
                    ToTransportMsg msg = ToTransportMsg.newBuilder()
                            .setSessionIdMSB(kv.getKey().getMostSignificantBits())
                            .setSessionIdLSB(kv.getKey().getLeastSignificantBits())
                            .setUplinkNotificationMsg(uplinkNotificationMsg)
                            .build();
                    systemContext.getJnksIotCoreToTransportService().process(kv.getValue().getSessionInfo().getNodeId(), msg);
                });
    }

    private void handleClaimDeviceMsg(SessionInfoProto sessionInfo, ClaimDeviceMsg msg) {
        UUID sessionId = getSessionId(sessionInfo);
        DeviceId deviceId = new DeviceId(new UUID(msg.getDeviceIdMSB(), msg.getDeviceIdLSB()));
        ListenableFuture<Void> registrationFuture = systemContext.getClaimDevicesService()
                        .registerClaimingInfo(tenantId, deviceId, msg.getSecretKey(), msg.getDurationMs());
        Futures.addCallback(registrationFuture, new FutureCallback<>() {
            @Override
            public void onSuccess(Void result) {
                log.debug("[{}][{}] Successfully processed register claiming info request!", sessionId, deviceId);
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("[{}][{}] Failed to process register claiming info request due to: ", sessionId, deviceId, t);
            }
        }, MoreExecutors.directExecutor());
    }

    private void reportSessionOpen() {
        systemContext.getDeviceStateService().onDeviceConnect(tenantId, deviceId);
    }

    private void reportSessionClose() {
        systemContext.getDeviceStateService().onDeviceDisconnect(tenantId, deviceId);
    }

    private void handleGetAttributesRequest(SessionInfoProto sessionInfo, GetAttributeRequestMsg request) {
        int requestId = request.getRequestId();
        if (request.getOnlyShared()) {
            Futures.addCallback(findAllAttributesByScope(AttributeScope.SHARED_SCOPE), new FutureCallback<>() {
                @Override
                public void onSuccess(@Nullable List<AttributeKvEntry> result) {
                    GetAttributeResponseMsg responseMsg = GetAttributeResponseMsg.newBuilder()
                            .setRequestId(requestId)
                            .setSharedStateMsg(true)
                            .addAllSharedAttributeList(KvProtoUtil.attrToTsKvProtos(result))
                            .setIsMultipleAttributesRequest(request.getSharedAttributeNamesCount() > 1)
                            .build();
                    sendToTransport(responseMsg, sessionInfo);
                }

                @Override
                public void onFailure(Throwable t) {
                    GetAttributeResponseMsg responseMsg = GetAttributeResponseMsg.newBuilder()
                            .setError(t.getMessage())
                            .setSharedStateMsg(true)
                            .build();
                    sendToTransport(responseMsg, sessionInfo);
                }
            }, MoreExecutors.directExecutor());
        } else {
            Futures.addCallback(getAttributesKvEntries(request), new FutureCallback<>() {
                @Override
                public void onSuccess(@Nullable List<List<AttributeKvEntry>> result) {
                    GetAttributeResponseMsg responseMsg = GetAttributeResponseMsg.newBuilder()
                            .setRequestId(requestId)
                            .addAllClientAttributeList(KvProtoUtil.attrToTsKvProtos(result.get(0)))
                            .addAllSharedAttributeList(KvProtoUtil.attrToTsKvProtos(result.get(1)))
                            .setIsMultipleAttributesRequest(
                                    request.getSharedAttributeNamesCount() + request.getClientAttributeNamesCount() > 1)
                            .build();
                    sendToTransport(responseMsg, sessionInfo);
                }

                @Override
                public void onFailure(Throwable t) {
                    GetAttributeResponseMsg responseMsg = GetAttributeResponseMsg.newBuilder()
                            .setError(t.getMessage())
                            .build();
                    sendToTransport(responseMsg, sessionInfo);
                }
            }, MoreExecutors.directExecutor());
        }
    }

    private ListenableFuture<List<List<AttributeKvEntry>>> getAttributesKvEntries(GetAttributeRequestMsg request) {
        ListenableFuture<List<AttributeKvEntry>> clientAttributesFuture;
        ListenableFuture<List<AttributeKvEntry>> sharedAttributesFuture;
        if (CollectionUtils.isEmpty(request.getClientAttributeNamesList()) && CollectionUtils.isEmpty(request.getSharedAttributeNamesList())) {
            clientAttributesFuture = findAllAttributesByScope(AttributeScope.CLIENT_SCOPE);
            sharedAttributesFuture = findAllAttributesByScope(AttributeScope.SHARED_SCOPE);
        } else if (!CollectionUtils.isEmpty(request.getClientAttributeNamesList()) && !CollectionUtils.isEmpty(request.getSharedAttributeNamesList())) {
            clientAttributesFuture = findAttributesByScope(toSet(request.getClientAttributeNamesList()), AttributeScope.CLIENT_SCOPE);
            sharedAttributesFuture = findAttributesByScope(toSet(request.getSharedAttributeNamesList()), AttributeScope.SHARED_SCOPE);
        } else if (CollectionUtils.isEmpty(request.getClientAttributeNamesList()) && !CollectionUtils.isEmpty(request.getSharedAttributeNamesList())) {
            clientAttributesFuture = Futures.immediateFuture(Collections.emptyList());
            sharedAttributesFuture = findAttributesByScope(toSet(request.getSharedAttributeNamesList()), AttributeScope.SHARED_SCOPE);
        } else {
            sharedAttributesFuture = Futures.immediateFuture(Collections.emptyList());
            clientAttributesFuture = findAttributesByScope(toSet(request.getClientAttributeNamesList()), AttributeScope.CLIENT_SCOPE);
        }
        return Futures.allAsList(Arrays.asList(clientAttributesFuture, sharedAttributesFuture));
    }

    private ListenableFuture<List<AttributeKvEntry>> findAllAttributesByScope(AttributeScope scope) {
        return systemContext.getAttributesService().findAll(tenantId, deviceId, scope);
    }

    private ListenableFuture<List<AttributeKvEntry>> findAttributesByScope(Set<String> attributesSet, AttributeScope scope) {
        return systemContext.getAttributesService().find(tenantId, deviceId, scope, attributesSet);
    }

    private Set<String> toSet(List<String> strings) {
        return new HashSet<>(strings);
    }

    /**
     * 会话类型。在 {@link #sessions} 里的按 ASYNC（与原行为一致）；不在里面的才回落到
     * **订阅里记录的类型**。
     * <p>
     * 原先一律"不在 {@code sessions} 里就算 SYNC"，这对"只订阅、不发 OPEN"的**虚拟会话**是错的 ——
     * 例如 UDP 为被动设备建的出站会话（{@code UdpOutboundTransportContext}）永远不入 {@code sessions}，
     * 于是被当成 SYNC；而 {@link #sendPendingRequests} 见到 SYNC 就把它的 RPC 订阅摘掉，
     * 结果这条通道每建立一次只投得出一条 RPC（实测：被动设备的 5 秒定时 RPC 只到 1 帧）。
     */
    private SessionType getSessionType(UUID sessionId) {
        if (sessions.containsKey(sessionId)) {
            return SessionType.ASYNC;
        }
        SessionInfo subscription = rpcSubscriptions.get(sessionId);
        return subscription != null ? subscription.getType() : SessionType.SYNC;
    }

    void processAttributesUpdate(DeviceAttributesEventNotificationMsg msg) {
        if (!attributeSubscriptions.isEmpty()) {
            boolean hasNotificationData = false;
            AttributeUpdateNotificationMsg.Builder notification = AttributeUpdateNotificationMsg.newBuilder();
            if (msg.isDeleted()) {
                List<String> sharedKeys = msg.getDeletedKeys().stream()
                        .filter(key -> DataConstants.SHARED_SCOPE.equals(key.getScope()))
                        .map(AttributeKey::getAttributeKey)
                        .collect(Collectors.toList());
                if (!sharedKeys.isEmpty()) {
                    notification.addAllSharedDeleted(sharedKeys);
                    hasNotificationData = true;
                }
            } else {
                if (DataConstants.SHARED_SCOPE.equals(msg.getScope())) {
                    List<AttributeKvEntry> attributes = new ArrayList<>(msg.getValues());
                    if (!attributes.isEmpty()) {
                        List<TsKvProto> sharedUpdated = msg.getValues().stream().map(t -> KvProtoUtil.toTsKvProto(t.getLastUpdateTs(), t))
                                .collect(Collectors.toList());
                        if (!sharedUpdated.isEmpty()) {
                            notification.addAllSharedUpdated(sharedUpdated);
                            hasNotificationData = true;
                        }
                    } else {
                        log.debug("[{}] No public shared side attributes changed!", deviceId);
                    }
                }
            }
            if (hasNotificationData) {
                AttributeUpdateNotificationMsg finalNotification = notification.build();
                attributeSubscriptions.forEach((key, value) -> sendToTransport(finalNotification, key, value.getNodeId()));
            }
        } else {
            log.debug("[{}] No registered attributes subscriptions to process!", deviceId);
        }
    }

    private void processRpcResponses(SessionInfoProto sessionInfo, ToDeviceRpcResponseMsg responseMsg) {
        UUID sessionId = getSessionId(sessionInfo);
        log.debug("[{}][{}] Processing RPC command response: {}", deviceId, sessionId, responseMsg);
        int requestId = responseMsg.getRequestId();
        ToDeviceRpcRequestMetadata requestMd = toDeviceRpcPendingMap.remove(requestId);
        boolean success = requestMd != null;
        if (success) {
            ToDeviceRpcRequest toDeviceRequestMsg = requestMd.getMsg().getMsg();
            UUID rpcId = toDeviceRequestMsg.getId();
            boolean delivered = requestMd.isDelivered();
            boolean hasError = StringUtils.isNotEmpty(responseMsg.getError());
            try {
                String payload = hasError ? responseMsg.getError() : responseMsg.getPayload();
                systemContext.getJnksIotCoreDeviceRpcService().processRpcResponseFromDeviceActor(
                        new FromDeviceRpcResponse(rpcId, payload, null));
                if (toDeviceRequestMsg.isPersisted()) {
                    RpcStatus status = hasError ? RpcStatus.FAILED : RpcStatus.SUCCESSFUL;
                    JsonNode response;
                    try {
                        response = JacksonUtil.toJsonNode(payload);
                    } catch (IllegalArgumentException e) {
                        response = JacksonUtil.newObjectNode().put("error", payload);
                    }
                    systemContext.getJnksIotRpcService().save(tenantId, new RpcId(rpcId), status, response);
                }
            } finally {
                if (rpcSubmitStrategy.equals(RpcSubmitStrategy.SEQUENTIAL_ON_RESPONSE_FROM_DEVICE)) {
                    clearAwaitRpcResponseScheduler();
                    String errorResponse = hasError ? "error response" : "response";
                    String rpcState = delivered ? "" : "undelivered ";
                    sendNextPendingRequest(rpcId, requestId, String.format("Received %s for %sRPC!", errorResponse, rpcState));
                } else if (!delivered) {
                    String errorResponse = hasError ? "error response" : "response";
                    sendNextPendingRequest(rpcId, requestId, String.format("Received %s for undelivered RPC!", errorResponse));
                }
            }
        } else {
            log.debug("[{}][{}][{}] RPC command response is stale!", deviceId, sessionId, requestId);
        }
    }

    private void processRpcResponseStatus(SessionInfoProto sessionInfo, ToDeviceRpcResponseStatusMsg responseMsg) {
        UUID rpcId = new UUID(responseMsg.getRequestIdMSB(), responseMsg.getRequestIdLSB());
        RpcStatus status = RpcStatus.valueOf(responseMsg.getStatus());
        UUID sessionId = getSessionId(sessionInfo);
        int requestId = responseMsg.getRequestId();
        log.debug("[{}][{}][{}][{}] Processing RPC command response status: [{}]", deviceId, sessionId, rpcId, requestId, status);
        ToDeviceRpcRequestMetadata md = toDeviceRpcPendingMap.get(requestId);
        if (md != null) {
            var toDeviceRpcRequest = md.getMsg().getMsg();
            boolean persisted = toDeviceRpcRequest.isPersisted();
            boolean oneWayRpc = toDeviceRpcRequest.isOneway();
            JsonNode response = null;
            if (status.equals(RpcStatus.DELIVERED)) {
                if (oneWayRpc) {
                    toDeviceRpcPendingMap.remove(requestId);
                    if (rpcSequential) {
                        var fromDeviceRpcResponse = new FromDeviceRpcResponse(rpcId, null, null);
                        systemContext.getJnksIotCoreDeviceRpcService().processRpcResponseFromDeviceActor(fromDeviceRpcResponse);
                    }
                } else {
                    md.setDelivered(true);
                    if (rpcSubmitStrategy.equals(RpcSubmitStrategy.SEQUENTIAL_ON_RESPONSE_FROM_DEVICE)) {
                        awaitRpcResponseFuture = scheduleAwaitRpcResponseFuture(rpcId, requestId);
                    }
                }
            } else if (status.equals(RpcStatus.TIMEOUT)) {
                Integer maxRpcRetries = toDeviceRpcRequest.getRetries();
                maxRpcRetries = maxRpcRetries == null ?
                        systemContext.getMaxRpcRetries() : Math.min(maxRpcRetries, systemContext.getMaxRpcRetries());
                if (maxRpcRetries <= md.getRetries()) {
                    if (closeTransportSessionOnRpcDeliveryTimeout) {
                        md.setRetries(0);
                        status = RpcStatus.QUEUED;
                        notifyTransportAboutSessionsCloseAndDumpSessions(TransportSessionCloseReason.RPC_DELIVERY_TIMEOUT);
                    } else {
                        toDeviceRpcPendingMap.remove(requestId);
                        status = RpcStatus.FAILED;
                        response = JacksonUtil.newObjectNode().put("error", "There was a Timeout and all retry " +
                                                                            "attempts have been exhausted. Retry attempts set: " + maxRpcRetries);
                    }
                } else {
                    md.setRetries(md.getRetries() + 1);
                }
            }

            if (persisted) {
                systemContext.getJnksIotRpcService().save(tenantId, new RpcId(rpcId), status, response);
            }
            if (rpcSubmitStrategy.equals(RpcSubmitStrategy.SEQUENTIAL_ON_RESPONSE_FROM_DEVICE)
                    && status.equals(RpcStatus.DELIVERED) && !oneWayRpc) {
                return;
            }
            if (!status.equals(RpcStatus.SENT)) {
                sendNextPendingRequest(rpcId, requestId, String.format("RPC was %s!", status.name().toLowerCase()));
            }
        } else {
            log.warn("[{}][{}][{}][{}] RPC has already been removed from pending map.", deviceId, sessionId, rpcId, requestId);
        }
    }

    private void processSubscriptionCommands(SessionInfoProto sessionInfo, SubscribeToAttributeUpdatesMsg subscribeCmd) {
        UUID sessionId = getSessionId(sessionInfo);
        if (subscribeCmd.getUnsubscribe()) {
            log.debug("[{}] Canceling attributes subscription for session: [{}]", deviceId, sessionId);
            attributeSubscriptions.remove(sessionId);
            dumpSessions();
        } else {
            SessionInfoMetaData sessionMD = sessions.get(sessionId);
            if (sessionMD == null) {
                sessionMD = new SessionInfoMetaData(new SessionInfo(subscribeCmd.getSessionType(), sessionInfo.getNodeId()));
            }
            sessionMD.setSubscribedToAttributes(true);
            log.debug("[{}] Registering attributes subscription for session: [{}]", deviceId, sessionId);
            attributeSubscriptions.put(sessionId, sessionMD.getSessionInfo());
            dumpSessions();
        }
    }

    private UUID getSessionId(SessionInfoProto sessionInfo) {
        return new UUID(sessionInfo.getSessionIdMSB(), sessionInfo.getSessionIdLSB());
    }

    private void processSubscriptionCommands(SessionInfoProto sessionInfo, SubscribeToRPCMsg subscribeCmd) {
        UUID sessionId = getSessionId(sessionInfo);
        if (subscribeCmd.getUnsubscribe()) {
            log.debug("[{}] Canceling RPC subscription for session: [{}]", deviceId, sessionId);
            rpcSubscriptions.remove(sessionId);
            clearAwaitRpcResponseScheduler();
            // 同步刷一次缓存：出站会话退场时会显式退订，不刷的话缓存里会留着这条已退掉的订阅，
            // Core 重启后又被恢复回来（见 dumpSubscriptionOnlySessions）。
            dumpSessions();
        } else {
            SessionInfoMetaData sessionMD = sessions.get(sessionId);
            if (sessionMD == null) {
                sessionMD = new SessionInfoMetaData(new SessionInfo(subscribeCmd.getSessionType(), sessionInfo.getNodeId()));
            }
            sessionMD.setSubscribedToRPC(true);
            rpcSubscriptions.put(sessionId, sessionMD.getSessionInfo());
            log.debug("[{}] Registered RPC subscription for session: [{}] Going to check for pending requests ...", deviceId, sessionId);
            sendPendingRequests(sessionId, sessionInfo.getNodeId());
            dumpSessions();
        }
    }

    private void processSessionStateMsgs(SessionInfoProto sessionInfo, SessionEventMsg msg) {
        UUID sessionId = getSessionId(sessionInfo);
        Objects.requireNonNull(sessionId);
        if (msg.getEvent() == SessionEvent.OPEN) {
            if (sessions.containsKey(sessionId)) {
                log.debug("[{}][{}] Received duplicate session open event.", deviceId, sessionId);
                return;
            }
            log.debug("[{}] Processing new session: [{}] Current sessions size: {}", deviceId, sessionId, sessions.size());

            sessions.put(sessionId, new SessionInfoMetaData(new SessionInfo(SessionType.ASYNC, sessionInfo.getNodeId())));
            if (sessions.size() == 1) {
                reportSessionOpen();
            }
            systemContext.getDeviceStateService().onDeviceActivity(tenantId, deviceId, System.currentTimeMillis());
            dumpSessions();
        } else if (msg.getEvent() == SessionEvent.CLOSED) {
            log.debug("[{}][{}] Canceling subscriptions for closed session.", deviceId, sessionId);
            sessions.remove(sessionId);
            attributeSubscriptions.remove(sessionId);
            rpcSubscriptions.remove(sessionId);
            clearAwaitRpcResponseScheduler();
            if (sessions.isEmpty()) {
                reportSessionClose();
                // 不在这里标非活跃。MQTT 服务端断开有 5 秒闪断窗口，
                // 非活跃由传输层 delay / 重启 flush / 会话超时兜底上报。
            }
            dumpSessions();
        }
    }

    private ScheduledFuture<?> scheduleAwaitRpcResponseFuture(UUID rpcId, int requestId) {
        return scheduler.schedule(() -> {
            var md = toDeviceRpcPendingMap.remove(requestId);
            if (md == null) {
                return;
            }
            sendNextPendingRequest(rpcId, requestId, "RPC was removed from pending map due to await timeout on response from device!");
            var toDeviceRpcRequest = md.getMsg().getMsg();
            if (toDeviceRpcRequest.isPersisted()) {
                var responseAwaitTimeout = JacksonUtil.newObjectNode().put("error", "There was a timeout awaiting for RPC response from device.");
                systemContext.getJnksIotRpcService().save(tenantId, new RpcId(rpcId), RpcStatus.FAILED, responseAwaitTimeout);
            }
        }, systemContext.getRpcResponseTimeout(), TimeUnit.MILLISECONDS);
    }

    private void clearAwaitRpcResponseScheduler() {
        if (rpcSubmitStrategy.equals(RpcSubmitStrategy.SEQUENTIAL_ON_RESPONSE_FROM_DEVICE) && awaitRpcResponseFuture != null) {
            awaitRpcResponseFuture.cancel(true);
        }
    }

    private void handleSessionActivity(SessionInfoProto sessionInfoProto, SubscriptionInfoProto subscriptionInfo) {
        UUID sessionId = getSessionId(sessionInfoProto);
        Objects.requireNonNull(sessionId);

        SessionInfoMetaData sessionMD = sessions.get(sessionId);
        if (sessionMD != null) {
            sessionMD.setLastActivityTime(subscriptionInfo.getLastActivityTime());
            sessionMD.setSubscribedToAttributes(subscriptionInfo.getAttributeSubscription());
            sessionMD.setSubscribedToRPC(subscriptionInfo.getRpcSubscription());
            if (subscriptionInfo.getAttributeSubscription()) {
                attributeSubscriptions.putIfAbsent(sessionId, sessionMD.getSessionInfo());
            }
            if (subscriptionInfo.getRpcSubscription()) {
                rpcSubscriptions.putIfAbsent(sessionId, sessionMD.getSessionInfo());
            }
        }
        // 这里**不再**记活动：会话打开时（processSessionStateMsgs 的 OPEN 分支）已经记过一次，
        // 对真实设备是重复记；而 UDP 的"出站会话"（为从不发包的被动设备建的下发通道）**不发 OPEN**，
        // 只发订阅 —— 在这里记活动会把"一个包都没发"的设备标成活跃，与"收到数据才算活跃"矛盾。
        if (sessionMD != null) {
            dumpSessions();
        }
    }

    void processCredentialsUpdate(JnksIotActorMsg msg) {
        if (((DeviceCredentialsUpdateNotificationMsg) msg).getDeviceCredentials().getCredentialsType() == DeviceCredentialsType.LWM2M_CREDENTIALS) {
            sessions.forEach((k, v) ->
                    notifyTransportAboutDeviceCredentialsUpdate(k, v, ((DeviceCredentialsUpdateNotificationMsg) msg).getDeviceCredentials()));
        } else {
            notifyTransportAboutSessionsCloseAndDumpSessions(TransportSessionCloseReason.CREDENTIALS_UPDATED);
        }
    }

    private void notifyTransportAboutSessionsCloseAndDumpSessions(TransportSessionCloseReason transportSessionCloseReason) {
        sessions.forEach((sessionId, sessionMd) -> notifyTransportAboutClosedSession(sessionId, sessionMd, transportSessionCloseReason));
        attributeSubscriptions.clear();
        rpcSubscriptions.clear();
        dumpSessions();
    }

    private void notifyTransportAboutClosedSessionMaxSessionsLimit(UUID sessionId, SessionInfoMetaData sessionMd) {
        attributeSubscriptions.remove(sessionId);
        rpcSubscriptions.remove(sessionId);
        notifyTransportAboutClosedSession(sessionId, sessionMd, TransportSessionCloseReason.MAX_CONCURRENT_SESSIONS_LIMIT_REACHED);
    }

    private void notifyTransportAboutClosedSession(UUID sessionId, SessionInfoMetaData sessionMd, TransportSessionCloseReason transportSessionCloseReason) {
        log.debug("{} sessionId: [{}] sessionMd: [{}]", transportSessionCloseReason.getLogMessage(), sessionId, sessionMd);
        SessionCloseNotificationProto sessionCloseNotificationProto = SessionCloseNotificationProto
                .newBuilder()
                .setMessage(transportSessionCloseReason.getNotificationMessage())
                .setReason(SessionCloseReason.forNumber(transportSessionCloseReason.getProtoNumber()))
                .build();
        ToTransportMsg msg = ToTransportMsg.newBuilder()
                .setSessionIdMSB(sessionId.getMostSignificantBits())
                .setSessionIdLSB(sessionId.getLeastSignificantBits())
                .setSessionCloseNotification(sessionCloseNotificationProto)
                .build();
        systemContext.getJnksIotCoreToTransportService().process(sessionMd.getSessionInfo().getNodeId(), msg);
    }

    void notifyTransportAboutDeviceCredentialsUpdate(UUID sessionId, SessionInfoMetaData sessionMd, DeviceCredentials deviceCredentials) {
        ToTransportUpdateCredentialsProto.Builder notification = ToTransportUpdateCredentialsProto.newBuilder();
        notification.addCredentialsId(deviceCredentials.getCredentialsId());
        notification.addCredentialsValue(deviceCredentials.getCredentialsValue());
        ToTransportMsg msg = ToTransportMsg.newBuilder()
                .setSessionIdMSB(sessionId.getMostSignificantBits())
                .setSessionIdLSB(sessionId.getLeastSignificantBits())
                .setToTransportUpdateCredentialsNotification(notification).build();
        systemContext.getJnksIotCoreToTransportService().process(sessionMd.getSessionInfo().getNodeId(), msg);
    }

    void processNameOrTypeUpdate(DeviceNameOrTypeUpdateMsg msg) {
        this.deviceName = msg.getDeviceName();
        this.deviceType = msg.getDeviceType();
        this.defaultMetaData = new JnksIotMsgMetaData();
        this.defaultMetaData.putValue("deviceName", deviceName);
        this.defaultMetaData.putValue("deviceType", deviceType);
    }

    private void sendToTransport(GetAttributeResponseMsg responseMsg, SessionInfoProto sessionInfo) {
        ToTransportMsg msg = ToTransportMsg.newBuilder()
                .setSessionIdMSB(sessionInfo.getSessionIdMSB())
                .setSessionIdLSB(sessionInfo.getSessionIdLSB())
                .setGetAttributesResponse(responseMsg).build();
        systemContext.getJnksIotCoreToTransportService().process(sessionInfo.getNodeId(), msg);
    }

    private void sendToTransport(AttributeUpdateNotificationMsg notificationMsg, UUID sessionId, String nodeId) {
        ToTransportMsg msg = ToTransportMsg.newBuilder()
                .setSessionIdMSB(sessionId.getMostSignificantBits())
                .setSessionIdLSB(sessionId.getLeastSignificantBits())
                .setAttributeUpdateNotification(notificationMsg).build();
        systemContext.getJnksIotCoreToTransportService().process(nodeId, msg);
    }

    private void sendToTransport(ToDeviceRpcRequestMsg rpcMsg, UUID sessionId, String nodeId) {
        ToTransportMsg msg = ToTransportMsg.newBuilder()
                .setSessionIdMSB(sessionId.getMostSignificantBits())
                .setSessionIdLSB(sessionId.getLeastSignificantBits())
                .setToDeviceRequest(rpcMsg).build();
        systemContext.getJnksIotCoreToTransportService().process(nodeId, msg);
    }

    void restoreSessions() {
        if (systemContext.isLocalCacheType()) {
            return;
        }
        log.debug("[{}] Restoring sessions from cache", deviceId);
        DeviceSessionsCacheEntry sessionsDump;
        try {
            sessionsDump = systemContext.getDeviceSessionCacheService().get(deviceId);
        } catch (Exception e) {
            log.warn("[{}] Failed to decode device sessions from cache", deviceId);
            return;
        }
        if (sessionsDump.getSessionsCount() == 0) {
            log.debug("[{}] No session information found", deviceId);
            return;
        }
        // TODO: Take latest max allowed sessions size from cache
        for (SessionSubscriptionInfoProto sessionSubscriptionInfoProto : sessionsDump.getSessionsList()) {
            SessionInfoProto sessionInfoProto = sessionSubscriptionInfoProto.getSessionInfo();
            UUID sessionId = getSessionId(sessionInfoProto);
            SessionInfo sessionInfo = new SessionInfo(SessionType.ASYNC, sessionInfoProto.getNodeId());
            SubscriptionInfoProto subInfo = sessionSubscriptionInfoProto.getSubscriptionInfo();
            // "只订阅、没发过 OPEN"的虚拟会话（如 UDP 出站会话）只还原订阅，**不**放进 sessions ——
            // 它本来就不在那里，放进去会占用并发上限、并让设备被判活跃。
            if (sessionSubscriptionInfoProto.getSubscriptionOnly()) {
                if (subInfo.getAttributeSubscription()) {
                    attributeSubscriptions.put(sessionId, sessionInfo);
                }
                if (subInfo.getRpcSubscription()) {
                    rpcSubscriptions.put(sessionId, sessionInfo);
                }
                log.debug("[{}] Restored subscription-only session: {}", deviceId, sessionId);
                continue;
            }
            SessionInfoMetaData sessionMD = new SessionInfoMetaData(sessionInfo, subInfo.getLastActivityTime());
            sessions.put(sessionId, sessionMD);
            if (subInfo.getAttributeSubscription()) {
                attributeSubscriptions.put(sessionId, sessionInfo);
                sessionMD.setSubscribedToAttributes(true);
            }
            if (subInfo.getRpcSubscription()) {
                rpcSubscriptions.put(sessionId, sessionInfo);
                sessionMD.setSubscribedToRPC(true);
            }
            log.debug("[{}] Restored session: {}", deviceId, sessionMD);
        }
        log.debug("[{}] Restored sessions: {}, RPC subscriptions: {}, attribute subscriptions: {}", deviceId, sessions.size(), rpcSubscriptions.size(), attributeSubscriptions.size());
    }

    private void dumpSessions() {
        if (systemContext.isLocalCacheType()) {
            return;
        }
        log.debug("[{}] Dumping sessions: {}, RPC subscriptions: {}, attribute subscriptions: {} to cache", deviceId, sessions.size(), rpcSubscriptions.size(), attributeSubscriptions.size());
        List<SessionSubscriptionInfoProto> sessionsList = new ArrayList<>(sessions.size());
        sessions.forEach((uuid, sessionMD) -> {
            if (sessionMD.getSessionInfo().getType() == SessionType.SYNC) {
                return;
            }
            SessionInfo sessionInfo = sessionMD.getSessionInfo();
            SubscriptionInfoProto subscriptionInfoProto = SubscriptionInfoProto.newBuilder()
                    .setLastActivityTime(sessionMD.getLastActivityTime())
                    .setAttributeSubscription(sessionMD.isSubscribedToAttributes())
                    .setRpcSubscription(sessionMD.isSubscribedToRPC()).build();
            SessionInfoProto sessionInfoProto = SessionInfoProto.newBuilder()
                    .setSessionIdMSB(uuid.getMostSignificantBits())
                    .setSessionIdLSB(uuid.getLeastSignificantBits())
                    .setNodeId(sessionInfo.getNodeId()).build();
            sessionsList.add(SessionSubscriptionInfoProto.newBuilder()
                    .setSessionInfo(sessionInfoProto)
                    .setSubscriptionInfo(subscriptionInfoProto).build());
            log.debug("[{}] Dumping session: {}", deviceId, sessionMD);
        });
        dumpSubscriptionOnlySessions(sessionsList);
        systemContext.getDeviceSessionCacheService()
                .put(deviceId, DeviceSessionsCacheEntry.newBuilder()
                        .addAllSessions(sessionsList).build());
    }

    /**
     * 把"只在订阅表里、不在 {@link #sessions} 里"的**虚拟会话**也写进缓存。
     * <p>
     * 目前唯一的来源是 UDP 为被动设备建的出站会话（{@code UdpOutboundTransportContext}）——
     * 它**只发 RPC 订阅、不发 {@code SESSION_EVENT_MSG_OPEN}**（为的是不被 {@code onDeviceActivity}
     * 把"一个包都没发"的设备标成活跃），所以永远不在 {@code sessions} 里。
     * 不把它一起缓存的话，Core 重启后 {@link #restoreSessions()} 恢复不出这条订阅，
     * 该设备的 RPC 就再也投不出去（实测：被动设备的定时 RPC 在 Core 重启后永久失效）。
     */
    private void dumpSubscriptionOnlySessions(List<SessionSubscriptionInfoProto> sessionsList) {
        Set<UUID> subscriptionOnlyIds = new HashSet<>(rpcSubscriptions.keySet());
        subscriptionOnlyIds.addAll(attributeSubscriptions.keySet());
        subscriptionOnlyIds.removeAll(sessions.keySet());
        for (UUID sessionId : subscriptionOnlyIds) {
            SessionInfo sessionInfo = rpcSubscriptions.getOrDefault(sessionId, attributeSubscriptions.get(sessionId));
            if (sessionInfo == null || sessionInfo.getNodeId() == null) {
                continue;
            }
            sessionsList.add(SessionSubscriptionInfoProto.newBuilder()
                    .setSessionInfo(SessionInfoProto.newBuilder()
                            .setSessionIdMSB(sessionId.getMostSignificantBits())
                            .setSessionIdLSB(sessionId.getLeastSignificantBits())
                            .setNodeId(sessionInfo.getNodeId()).build())
                    .setSubscriptionInfo(SubscriptionInfoProto.newBuilder()
                            .setLastActivityTime(0)
                            .setAttributeSubscription(attributeSubscriptions.containsKey(sessionId))
                            .setRpcSubscription(rpcSubscriptions.containsKey(sessionId)).build())
                    .setSubscriptionOnly(true)
                    .build());
            log.debug("[{}] Dumping subscription-only session: {}", deviceId, sessionId);
        }
    }

    void init(JnksIotActorCtx ctx) {
        PageLink pageLink = new PageLink(1024, 0, null, new SortOrder("createdTime"));
        PageData<Rpc> pageData;
        do {
            pageData = systemContext.getJnksIotRpcService().findAllByDeviceIdAndStatus(tenantId, deviceId, RpcStatus.QUEUED, pageLink);
            pageData.getData().forEach(rpc -> {
                ToDeviceRpcRequest msg = JacksonUtil.convertValue(rpc.getRequest(), ToDeviceRpcRequest.class);
                long timeout = rpc.getExpirationTime() - System.currentTimeMillis();
                if (timeout <= 0) {
                    rpc.setStatus(RpcStatus.EXPIRED);
                    systemContext.getJnksIotRpcService().save(tenantId, rpc);
                } else {
                    registerPendingRpcRequest(ctx, new ToDeviceRpcRequestActorMsg(systemContext.getServiceId(), msg), false, createToDeviceRpcRequestMsg(msg), timeout);
                }
            });
            if (pageData.hasNext()) {
                pageLink = pageLink.nextPageLink();
            }
        } while (pageData.hasNext());
    }

    void checkSessionsTimeout() {
        final long expTime = System.currentTimeMillis() - systemContext.getSessionInactivityTimeout();
        Set<String> liveTransports = Set.of();
        try {
            liveTransports = systemContext.getPartitionService().getAllServiceIds(ServiceType.JNKS_IOT_TRANSPORT);
        } catch (Exception e) {
            log.debug("[{}] Failed to resolve live transport nodes", deviceId, e);
        }
        List<UUID> expiredIds = null;

        for (Map.Entry<UUID, SessionInfoMetaData> kv : sessions.entrySet()) { //entry set are cached for stable sessions
            SessionInfoMetaData sessionMd = kv.getValue();
            boolean timedOut = sessionMd.getLastActivityTime() < expTime;
            String nodeId = sessionMd.getSessionInfo() != null ? sessionMd.getSessionInfo().getNodeId() : null;
            boolean transportGone = liveTransports != null && !liveTransports.isEmpty()
                    && nodeId != null && !liveTransports.contains(nodeId);
            if (timedOut || transportGone) {
                final UUID id = kv.getKey();
                if (expiredIds == null) {
                    expiredIds = new ArrayList<>(1); //most of the expired sessions is a single event
                }
                expiredIds.add(id);
            }
        }

        if (expiredIds != null) {
            int removed = 0;
            for (UUID id : expiredIds) {
                final SessionInfoMetaData session = sessions.remove(id);
                rpcSubscriptions.remove(id);
                attributeSubscriptions.remove(id);
                if (session != null) {
                    removed++;
                    notifyTransportAboutClosedSession(id, session, TransportSessionCloseReason.SESSION_TIMEOUT);
                }
            }
            if (removed != 0) {
                if (sessions.isEmpty()) {
                    reportSessionClose();
                    systemContext.getDeviceStateService().onLastSessionClosed(tenantId, deviceId);
                }
                dumpSessions();
            }
        }

    }

}
