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
package org.thingsboard.server.service.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.common.util.ThingsBoardExecutors;
import org.thingsboard.server.common.data.AttributeScope;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.TenantProfile;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.EntityIdFactory;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.id.UserId;
import org.thingsboard.server.common.data.kv.Aggregation;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
import org.thingsboard.server.common.data.kv.BaseReadTsKvQuery;
import org.thingsboard.server.common.data.kv.BasicTsKvEntry;
import org.thingsboard.server.common.data.kv.ReadTsKvQuery;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.data.tenant.profile.DefaultTenantProfileConfiguration;
import org.thingsboard.server.common.msg.tools.TbRateLimitsException;
import org.thingsboard.server.dao.attributes.AttributesService;
import org.thingsboard.server.dao.tenant.TbTenantProfileCache;
import org.thingsboard.server.dao.timeseries.TimeseriesService;
import org.thingsboard.server.dao.util.TenantRateLimitException;
import org.thingsboard.server.exception.UnauthorizedException;
import org.thingsboard.server.queue.discovery.TbServiceInfoProvider;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.security.AccessValidator;
import org.thingsboard.server.service.security.ValidationCallback;
import org.thingsboard.server.service.security.ValidationResult;
import org.thingsboard.server.service.security.ValidationResultCode;
import org.thingsboard.server.service.security.model.UserPrincipal;
import org.thingsboard.server.service.security.permission.Operation;
import org.thingsboard.server.service.subscription.SubscriptionErrorCode;
import org.thingsboard.server.service.subscription.TbAttributeSubscription;
import org.thingsboard.server.service.subscription.TbAttributeSubscriptionScope;
import org.thingsboard.server.service.subscription.TbEntityDataSubscriptionService;
import org.thingsboard.server.service.subscription.TbLocalSubscriptionService;
import org.thingsboard.server.service.subscription.TbTimeSeriesSubscription;
import org.thingsboard.server.service.ws.notification.NotificationCommandsHandler;
import org.thingsboard.server.service.ws.telemetry.cmd.v1.AttributesSubscriptionCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v1.GetHistoryCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v1.SubscriptionCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v1.TelemetryPluginCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v1.TimeseriesSubscriptionCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.AlarmCountCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.AlarmDataCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.AlarmStatusCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.CmdUpdate;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.EntityCountCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.EntityDataCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.EntityDataUpdate;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.UnsubscribeCmd;
import org.thingsboard.server.service.ws.telemetry.sub.TelemetrySubscriptionUpdate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.thingsboard.server.common.data.DataConstants.LATEST_TELEMETRY_SCOPE;

/**
 * ThingsBoard WebSocket 服务实现类
 * <p>
 * 作为WebSocket通信的总入口，负责管理客户端连接的生命周期（建立、维持、关闭）、
 * 验证会话、以及作为消息在服务器与前端间双向转发的枢纽。
 *
 * @author ashvayka
 * @since 27.03.18
 */
@Service
@TbCoreComponent
@Slf4j
@RequiredArgsConstructor
public class DefaultWebSocketService implements WebSocketService {

    /**
     * Ping 尝试次数
     */
    public static final int NUMBER_OF_PING_ATTEMPTS = 3;

    /**
     * 默认查询限制
     */
    private static final int DEFAULT_LIMIT = 100;

    /**
     * 默认数据聚合方式
     */
    private static final Aggregation DEFAULT_AGGREGATION = Aggregation.NONE;

    /**
     * 未知订阅ID
     */
    private static final int UNKNOWN_SUBSCRIPTION_ID = 0;

    /**
     * 日志处理消息模板
     */
    private static final String PROCESSING_MSG = "[{}] Processing: {}";

    /**
     * 错误消息常量
     */
    private static final String FAILED_TO_FETCH_DATA = "Failed to fetch data!";
    private static final String FAILED_TO_FETCH_ATTRIBUTES = "Failed to fetch attributes!";
    private static final String SESSION_META_DATA_NOT_FOUND = "Session meta-data not found!";

    /**
     * WebSocket 会话元数据映射表: sessionId -> WsSessionMetaData
     */
    private final ConcurrentMap<String, WsSessionMetaData> wsSessionsMap = new ConcurrentHashMap<>();

    /**
     * 旧版订阅服务
     */
    private final TbLocalSubscriptionService oldSubService;

    /**
     * 实体数据订阅服务
     */
    private final TbEntityDataSubscriptionService entityDataSubService;

    /**
     * 通知命令处理器
     */
    private final NotificationCommandsHandler notificationCmdsHandler;

    /**
     * WebSocket 消息端点
     */
    private final WebSocketMsgEndpoint msgEndpoint;

    /**
     * 访问验证器
     */
    private final AccessValidator accessValidator;

    /**
     * 属性服务
     */
    private final AttributesService attributesService;

    /**
     * 时间序列服务
     */
    private final TimeseriesService tsService;

    /**
     * 服务信息提供者
     */
    private final TbServiceInfoProvider serviceInfoProvider;

    /**
     * 租户配置缓存
     */
    private final TbTenantProfileCache tenantProfileCache;

    /**
     * WebSocket Ping 超时时间
     */
    @Value("${server.ws.ping_timeout:30000}")
    private long pingTimeout;

    /**
     * 各层级订阅关系映射，用于限制订阅数量
     */
    private final ConcurrentMap<TenantId, Set<String>> tenantSubscriptionsMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<CustomerId, Set<String>> customerSubscriptionsMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<UserId, Set<String>> regularUserSubscriptionsMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<UserId, Set<String>> publicUserSubscriptionsMap = new ConcurrentHashMap<>();

    /**
     * 会话命令映射: sessionId -> (cmdId -> subId)
     */
    private final ConcurrentMap<String, Map<Integer, Integer>> sessionCmdMap = new ConcurrentHashMap<>();

    /**
     * 命令处理线程池
     */
    private ExecutorService executor;

    /**
     * Ping 定时任务线程池
     */
    private ScheduledExecutorService pingExecutor;

    /**
     * 当前服务实例标识
     */
    private String serviceId;

    /**
     * 命令处理器映射: 命令类型 -> 对应的处理器
     */
    private Map<WsCmdType, WsCmdHandler<? extends WsCmd>> cmdsHandlers;

    /**
     * 服务初始化方法
     *
     * 1. 初始化服务ID和线程池
     * 2. 启动Ping定时任务
     * 3. 注册所有命令处理器
     */
    @PostConstruct
    public void init() {
        // 初始化基础组件
        serviceId = serviceInfoProvider.getServiceId();
        executor = ThingsBoardExecutors.newWorkStealingPool(50, getClass());

        // 启动Ping定时任务，定期向所有活跃会话发送Ping
        pingExecutor = ThingsBoardExecutors.newSingleThreadScheduledExecutor("telemetry-web-socket-ping");
        pingExecutor.scheduleWithFixedDelay(this::sendPing, pingTimeout / NUMBER_OF_PING_ATTEMPTS, pingTimeout / NUMBER_OF_PING_ATTEMPTS, TimeUnit.MILLISECONDS);

        // 注册所有命令处理器
        cmdsHandlers = new EnumMap<>(WsCmdType.class);

        // 遥测相关命令
        cmdsHandlers.put(WsCmdType.ATTRIBUTES, newCmdHandler(this::handleWsAttributesSubscriptionCmd));
        cmdsHandlers.put(WsCmdType.TIMESERIES, newCmdHandler(this::handleWsTimeseriesSubscriptionCmd));
        cmdsHandlers.put(WsCmdType.TIMESERIES_HISTORY, newCmdHandler(this::handleWsHistoryCmd));
        // 实体数据相关命令
        cmdsHandlers.put(WsCmdType.ENTITY_DATA, newCmdHandler(this::handleWsEntityDataCmd));
        cmdsHandlers.put(WsCmdType.ALARM_DATA, newCmdHandler(this::handleWsAlarmDataCmd));
        cmdsHandlers.put(WsCmdType.ENTITY_COUNT, newCmdHandler(this::handleWsEntityCountCmd));
        cmdsHandlers.put(WsCmdType.ALARM_COUNT, newCmdHandler(this::handleWsAlarmCountCmd));
        cmdsHandlers.put(WsCmdType.ALARM_STATUS, newCmdHandler(this::handleWsAlarmsStatusCmd));
        // 取消订阅命令
        cmdsHandlers.put(WsCmdType.ENTITY_DATA_UNSUBSCRIBE, newCmdHandler(this::handleWsDataUnsubscribeCmd));
        cmdsHandlers.put(WsCmdType.ALARM_DATA_UNSUBSCRIBE, newCmdHandler(this::handleWsDataUnsubscribeCmd));
        cmdsHandlers.put(WsCmdType.ENTITY_COUNT_UNSUBSCRIBE, newCmdHandler(this::handleWsDataUnsubscribeCmd));
        cmdsHandlers.put(WsCmdType.ALARM_COUNT_UNSUBSCRIBE, newCmdHandler(this::handleWsDataUnsubscribeCmd));
        cmdsHandlers.put(WsCmdType.ALARM_STATUS_UNSUBSCRIBE, newCmdHandler(this::handleWsDataUnsubscribeCmd));
        // 通知相关命令
        cmdsHandlers.put(WsCmdType.NOTIFICATIONS, newCmdHandler(notificationCmdsHandler::handleUnreadNotificationsSubCmd));
        cmdsHandlers.put(WsCmdType.NOTIFICATIONS_COUNT, newCmdHandler(notificationCmdsHandler::handleUnreadNotificationsCountSubCmd));
        cmdsHandlers.put(WsCmdType.MARK_NOTIFICATIONS_AS_READ, newCmdHandler(notificationCmdsHandler::handleMarkAsReadCmd));
        cmdsHandlers.put(WsCmdType.MARK_ALL_NOTIFICATIONS_AS_READ, newCmdHandler(notificationCmdsHandler::handleMarkAllAsReadCmd));
        cmdsHandlers.put(WsCmdType.NOTIFICATIONS_UNSUBSCRIBE, newCmdHandler(notificationCmdsHandler::handleUnsubCmd));
    }

    @PreDestroy
    public void shutdownExecutor() {
        if (pingExecutor != null) {
            pingExecutor.shutdownNow();
        }

        if (executor != null) {
            executor.shutdownNow();
        }
    }

    /**
     * 处理 WebSocket 会话事件
     *
     * @param sessionRef 会话引用
     * @param event 会话事件
     */
    @Override
    public void handleSessionEvent(WebSocketSessionRef sessionRef, SessionEvent event) {
        String sessionId = sessionRef.getSessionId();
        TenantId tenantId = sessionRef.getSecurityCtx().getTenantId();
        log.debug(PROCESSING_MSG, sessionId, event);
        switch (event.getEventType()) {
            case ESTABLISHED:
                // 新会话建立，保存会话元数据
                wsSessionsMap.put(sessionId, new WsSessionMetaData(sessionRef));
                break;
            case ERROR:
                // 会话错误，记录日志
                log.debug("[{}][{}] Unknown websocket session error: ", tenantId, sessionId,
                        event.getError().orElse(new RuntimeException("No error specified")));
                break;
            case CLOSED:
                // 会话关闭，清理资源
                cleanupSessionById(tenantId, sessionId);
                processSessionClose(sessionRef);
                break;
        }
    }

    /**
     * 处理来自客户端的命令
     *
     * @param sessionRef 会话引用
     * @param commandsWrapper 命令包装器
     */
    @Override
    public void handleCommands(WebSocketSessionRef sessionRef, WsCommandsWrapper commandsWrapper) {
        if (commandsWrapper == null || CollectionUtils.isEmpty(commandsWrapper.getCmds())) {
            return;
        }
        String sessionId = sessionRef.getSessionId();
        // 校验session元数据
        if (!validateSessionMetadata(sessionRef, UNKNOWN_SUBSCRIPTION_ID, sessionId)) {
            return;
        }

        // 依次处理每个命令
        for (WsCmd cmd : commandsWrapper.getCmds()) {
            log.debug("[{}][{}][{}] Processing cmd: {}", sessionId, cmd.getType(), cmd.getCmdId(), cmd);
            try {
                // 根据命令类型路由到对应的处理器
                Optional.ofNullable(cmdsHandlers.get(cmd.getType()))
                        .ifPresent(cmdHandler -> cmdHandler.handle(sessionRef, cmd));
            } catch (TbRateLimitsException e) {
                log.debug("{} Failed to handle WS cmd: {}", sessionRef, cmd, e);
            } catch (Exception e) {
                // 命令处理失败，发送错误响应
                sendError(sessionRef, cmd.getCmdId(), SubscriptionErrorCode.INTERNAL_ERROR, e.getMessage());
                log.error("{} Failed to handle WS cmd: {}", sessionRef, cmd, e);
            }
        }
    }

    private void handleWsEntityDataCmd(WebSocketSessionRef sessionRef, EntityDataCmd cmd) {
        if (validateSubscriptionCmd(sessionRef, cmd)) {
            entityDataSubService.handleCmd(sessionRef, cmd);
        }
    }

    private void handleWsEntityCountCmd(WebSocketSessionRef sessionRef, EntityCountCmd cmd) {
        if (validateSubscriptionCmd(sessionRef, cmd)) {
            entityDataSubService.handleCmd(sessionRef, cmd);
        }
    }

    private void handleWsAlarmDataCmd(WebSocketSessionRef sessionRef, AlarmDataCmd cmd) {
        if (validateSubscriptionCmd(sessionRef, cmd)) {
            entityDataSubService.handleCmd(sessionRef, cmd);
        }
    }

    private void handleWsDataUnsubscribeCmd(WebSocketSessionRef sessionRef, UnsubscribeCmd cmd) {
        entityDataSubService.cancelSubscription(sessionRef.getSessionId(), cmd);
    }

    private void handleWsAlarmCountCmd(WebSocketSessionRef sessionRef, AlarmCountCmd cmd) {
        if (validateCmd(sessionRef, cmd)) {
            entityDataSubService.handleCmd(sessionRef, cmd);
        }
    }

    private void handleWsAlarmsStatusCmd(WebSocketSessionRef sessionRef, AlarmStatusCmd cmd) {
        if (validateCmd(sessionRef, cmd)) {
            entityDataSubService.handleCmd(sessionRef, cmd);
        }
    }

    @Override
    public void sendUpdate(String sessionId, int cmdId, TelemetrySubscriptionUpdate update) {
        // We substitute the subscriptionId with cmdId for old-style subscriptions.
        doSendUpdate(sessionId, cmdId, update.copyWithNewSubscriptionId(cmdId));
    }

    @Override
    public void sendUpdate(String sessionId, CmdUpdate update) {
        doSendUpdate(sessionId, update.getCmdId(), update);
    }

    @Override
    public void sendError(WebSocketSessionRef sessionRef, int subId, SubscriptionErrorCode errorCode, String errorMsg) {
        TelemetrySubscriptionUpdate update = new TelemetrySubscriptionUpdate(subId, errorCode, errorMsg);
        sendUpdate(sessionRef, update);
    }

    private <T> void doSendUpdate(String sessionId, int cmdId, T update) {
        WsSessionMetaData md = wsSessionsMap.get(sessionId);
        if (md != null) {
            sendUpdate(md.getSessionRef(), cmdId, update);
        }
    }

    @Override
    public void close(String sessionId, CloseStatus status) {
        WsSessionMetaData md = wsSessionsMap.get(sessionId);
        if (md != null) {
            try {
                msgEndpoint.close(md.getSessionRef(), status);
            } catch (IOException e) {
                log.warn("[{}] Failed to send session close", sessionId, e);
            }
        }
    }

    @Override
    public void cleanupIfStale(TenantId tenantId, String sessionId) {
        if (!msgEndpoint.isOpen(sessionId)) {
            log.info("[{}] Cleaning up stale session ", sessionId);
            cleanupSessionById(tenantId, sessionId);
        }
    }

    private void processSessionClose(WebSocketSessionRef sessionRef) {
        var tenantProfileConfiguration = getTenantProfileConfiguration(sessionRef);
        if (tenantProfileConfiguration != null) {
            String sessionId = "[" + sessionRef.getSessionId() + "]";

            if (tenantProfileConfiguration.getMaxWsSubscriptionsPerTenant() > 0) {
                Set<String> tenantSubscriptions = tenantSubscriptionsMap.computeIfAbsent(sessionRef.getSecurityCtx().getTenantId(), id -> ConcurrentHashMap.newKeySet());
                synchronized (tenantSubscriptions) {
                    tenantSubscriptions.removeIf(subId -> subId.startsWith(sessionId));
                }
            }
            if (sessionRef.getSecurityCtx().isCustomerUser()) {
                if (tenantProfileConfiguration.getMaxWsSubscriptionsPerCustomer() > 0) {
                    Set<String> customerSessions = customerSubscriptionsMap.computeIfAbsent(sessionRef.getSecurityCtx().getCustomerId(), id -> ConcurrentHashMap.newKeySet());
                    synchronized (customerSessions) {
                        customerSessions.removeIf(subId -> subId.startsWith(sessionId));
                    }
                }
                if (tenantProfileConfiguration.getMaxWsSubscriptionsPerRegularUser() > 0 && UserPrincipal.Type.USER_NAME.equals(sessionRef.getSecurityCtx().getUserPrincipal().getType())) {
                    Set<String> regularUserSessions = regularUserSubscriptionsMap.computeIfAbsent(sessionRef.getSecurityCtx().getId(), id -> ConcurrentHashMap.newKeySet());
                    synchronized (regularUserSessions) {
                        regularUserSessions.removeIf(subId -> subId.startsWith(sessionId));
                    }
                }
                if (tenantProfileConfiguration.getMaxWsSubscriptionsPerPublicUser() > 0 && UserPrincipal.Type.PUBLIC_ID.equals(sessionRef.getSecurityCtx().getUserPrincipal().getType())) {
                    Set<String> publicUserSessions = publicUserSubscriptionsMap.computeIfAbsent(sessionRef.getSecurityCtx().getId(), id -> ConcurrentHashMap.newKeySet());
                    synchronized (publicUserSessions) {
                        publicUserSessions.removeIf(subId -> subId.startsWith(sessionId));
                    }
                }
            }
        }
    }

    private boolean processSubscription(WebSocketSessionRef sessionRef, SubscriptionCmd cmd) {
        var tenantProfileConfiguration = getTenantProfileConfiguration(sessionRef);
        if (tenantProfileConfiguration == null) return true;

        String subId = "[" + sessionRef.getSessionId() + "]:[" + cmd.getCmdId() + "]";
        try {
            if (tenantProfileConfiguration.getMaxWsSubscriptionsPerTenant() > 0) {
                Set<String> tenantSubscriptions = tenantSubscriptionsMap.computeIfAbsent(sessionRef.getSecurityCtx().getTenantId(), id -> ConcurrentHashMap.newKeySet());
                synchronized (tenantSubscriptions) {
                    if (cmd.isUnsubscribe()) {
                        tenantSubscriptions.remove(subId);
                    } else if (tenantSubscriptions.size() < tenantProfileConfiguration.getMaxWsSubscriptionsPerTenant()) {
                        tenantSubscriptions.add(subId);
                    } else {
                        log.info("[{}][{}][{}] Failed to start subscription. Max tenant subscriptions limit reached"
                                , sessionRef.getSecurityCtx().getTenantId(), sessionRef.getSecurityCtx().getId(), subId);
                        msgEndpoint.close(sessionRef, CloseStatus.POLICY_VIOLATION.withReason("Max tenant subscriptions limit reached!"));
                        return false;
                    }
                }
            }

            if (sessionRef.getSecurityCtx().isCustomerUser()) {
                if (tenantProfileConfiguration.getMaxWsSubscriptionsPerCustomer() > 0) {
                    Set<String> customerSessions = customerSubscriptionsMap.computeIfAbsent(sessionRef.getSecurityCtx().getCustomerId(), id -> ConcurrentHashMap.newKeySet());
                    synchronized (customerSessions) {
                        if (cmd.isUnsubscribe()) {
                            customerSessions.remove(subId);
                        } else if (customerSessions.size() < tenantProfileConfiguration.getMaxWsSubscriptionsPerCustomer()) {
                            customerSessions.add(subId);
                        } else {
                            log.info("[{}][{}][{}] Failed to start subscription. Max customer subscriptions limit reached"
                                    , sessionRef.getSecurityCtx().getTenantId(), sessionRef.getSecurityCtx().getId(), subId);
                            msgEndpoint.close(sessionRef, CloseStatus.POLICY_VIOLATION.withReason("Max customer subscriptions limit reached"));
                            return false;
                        }
                    }
                }
                if (tenantProfileConfiguration.getMaxWsSubscriptionsPerRegularUser() > 0 && UserPrincipal.Type.USER_NAME.equals(sessionRef.getSecurityCtx().getUserPrincipal().getType())) {
                    Set<String> regularUserSessions = regularUserSubscriptionsMap.computeIfAbsent(sessionRef.getSecurityCtx().getId(), id -> ConcurrentHashMap.newKeySet());
                    synchronized (regularUserSessions) {
                        if (regularUserSessions.size() < tenantProfileConfiguration.getMaxWsSubscriptionsPerRegularUser()) {
                            regularUserSessions.add(subId);
                        } else {
                            log.info("[{}][{}][{}] Failed to start subscription. Max regular user subscriptions limit reached"
                                    , sessionRef.getSecurityCtx().getTenantId(), sessionRef.getSecurityCtx().getId(), subId);
                            msgEndpoint.close(sessionRef, CloseStatus.POLICY_VIOLATION.withReason("Max regular user subscriptions limit reached"));
                            return false;
                        }
                    }
                }
                if (tenantProfileConfiguration.getMaxWsSubscriptionsPerPublicUser() > 0 && UserPrincipal.Type.PUBLIC_ID.equals(sessionRef.getSecurityCtx().getUserPrincipal().getType())) {
                    Set<String> publicUserSessions = publicUserSubscriptionsMap.computeIfAbsent(sessionRef.getSecurityCtx().getId(), id -> ConcurrentHashMap.newKeySet());
                    synchronized (publicUserSessions) {
                        if (publicUserSessions.size() < tenantProfileConfiguration.getMaxWsSubscriptionsPerPublicUser()) {
                            publicUserSessions.add(subId);
                        } else {
                            log.info("[{}][{}][{}] Failed to start subscription. Max public user subscriptions limit reached"
                                    , sessionRef.getSecurityCtx().getTenantId(), sessionRef.getSecurityCtx().getId(), subId);
                            msgEndpoint.close(sessionRef, CloseStatus.POLICY_VIOLATION.withReason("Max public user subscriptions limit reached"));
                            return false;
                        }
                    }
                }
            }
        } catch (IOException e) {
            log.warn("[{}] Failed to send session close:", sessionRef.getSessionId(), e);
            return false;
        }
        return true;
    }

    /**
     * 处理属性订阅命令
     * <p>
     * 支持按指定键订阅和订阅所有属性两种模式
     */
    private void handleWsAttributesSubscriptionCmd(WebSocketSessionRef sessionRef, AttributesSubscriptionCmd cmd) {
        // 检查订阅限制
        if (!processSubscription(sessionRef, cmd)) {
            return;
        }

        String sessionId = sessionRef.getSessionId();
        if (cmd.isUnsubscribe()) {
            // 取消订阅
            unsubscribe(sessionRef, cmd, sessionId);
        } else if (validateSubscriptionCmd(sessionRef, cmd)) {
            EntityId entityId = EntityIdFactory.getByTypeAndId(cmd.getEntityType(), cmd.getEntityId());
            log.debug("[{}] fetching latest attributes ({}) values for device: {}", sessionId, cmd.getKeys(), entityId);
            Optional<Set<String>> keysOptional = getKeys(cmd);
            if (keysOptional.isPresent()) {
                // 按指定键订阅
                List<String> keys = new ArrayList<>(keysOptional.get());
                handleWsAttributesSubscriptionByKeys(sessionRef, cmd, sessionId, entityId, keys);
            } else {
                // 订阅所有属性
                handleWsAttributesSubscription(sessionRef, cmd, sessionId, entityId);
            }
        }
    }

    private void handleWsAttributesSubscriptionByKeys(WebSocketSessionRef sessionRef,
                                                      AttributesSubscriptionCmd cmd, String sessionId, EntityId entityId,
                                                      List<String> keys) {
        long queryTs = System.currentTimeMillis();
        FutureCallback<List<AttributeKvEntry>> callback = new FutureCallback<>() {
            @Override
            public void onSuccess(List<AttributeKvEntry> data) {
                // 成功获取属性数据的处理逻辑
                List<TsKvEntry> attributesData = data.stream().map(d -> new BasicTsKvEntry(d.getLastUpdateTs(), d)).collect(Collectors.toList());

                Map<String, Long> subState = new HashMap<>(keys.size());
                keys.forEach(key -> subState.put(key, 0L));
                attributesData.forEach(v -> subState.put(v.getKey(), v.getTs()));

                TbAttributeSubscriptionScope scope = StringUtils.isEmpty(cmd.getScope()) ? TbAttributeSubscriptionScope.ANY_SCOPE : TbAttributeSubscriptionScope.valueOf(cmd.getScope());

                Lock subLock = new ReentrantLock();
                TbAttributeSubscription sub = TbAttributeSubscription.builder()
                        .serviceId(serviceId)
                        .sessionId(sessionId)
                        .subscriptionId(registerNewSessionSubId(sessionId, sessionRef, cmd.getCmdId()))
                        .tenantId(sessionRef.getSecurityCtx().getTenantId())
                        .entityId(entityId)
                        .queryTs(queryTs)
                        .allKeys(false)
                        .keyStates(subState)
                        .scope(scope)
                        .updateProcessor((subscription, update) -> {
                            // 属性更新时的回调处理器
                            subLock.lock();
                            try {
                                // 一更新发送数据到客户端
                                sendUpdate(subscription.getSessionId(), cmd.getCmdId(), update);
                            } finally {
                                subLock.unlock();
                            }
                        })
                        .build();

                subLock.lock();
                try {
                    // 添加订阅
                    oldSubService.addSubscription(sub, sessionRef);
                    // 发送初始数据到客户端
                    sendUpdate(sessionRef, new TelemetrySubscriptionUpdate(cmd.getCmdId(), attributesData));
                } finally {
                    subLock.unlock();
                }

            }

            @Override
            public void onFailure(Throwable e) {
                log.error(FAILED_TO_FETCH_ATTRIBUTES, e);
                TelemetrySubscriptionUpdate update;
                if (e instanceof UnauthorizedException) {
                    update = new TelemetrySubscriptionUpdate(cmd.getCmdId(), SubscriptionErrorCode.UNAUTHORIZED,
                            SubscriptionErrorCode.UNAUTHORIZED.getDefaultMsg());
                } else {
                    update = new TelemetrySubscriptionUpdate(cmd.getCmdId(), SubscriptionErrorCode.INTERNAL_ERROR,
                            FAILED_TO_FETCH_ATTRIBUTES);
                }
                sendUpdate(sessionRef, update);
            }
        };

        if (StringUtils.isEmpty(cmd.getScope())) {
            accessValidator.validate(sessionRef.getSecurityCtx(), Operation.READ_ATTRIBUTES, entityId, getAttributesFetchCallback(sessionRef.getSecurityCtx().getTenantId(), entityId, keys, callback));
        } else {
            accessValidator.validate(sessionRef.getSecurityCtx(), Operation.READ_ATTRIBUTES, entityId, getAttributesFetchCallback(sessionRef.getSecurityCtx().getTenantId(), entityId, cmd.getScope(), keys, callback));
        }
    }

    private int registerNewSessionSubId(String sessionId, WebSocketSessionRef sessionRef, int cmdId) {
        var cmdMap = sessionCmdMap.computeIfAbsent(sessionId, id -> new ConcurrentHashMap<>());
        var subId = sessionRef.getSessionSubIdSeq().incrementAndGet();
        cmdMap.put(cmdId, subId);
        return subId;
    }

    /**
     * 处理时间序列历史数据查询命令
     */
    private void handleWsHistoryCmd(WebSocketSessionRef sessionRef, GetHistoryCmd cmd) {
        if (!validateCmd(sessionRef, cmd, () -> {
            if (cmd.getEntityId() == null || cmd.getEntityId().isEmpty() || cmd.getEntityType() == null || cmd.getEntityType().isEmpty()) {
                throw new IllegalArgumentException("Device id is empty!");
            }
            if (cmd.getKeys() == null || cmd.getKeys().isEmpty()) {
                throw new IllegalArgumentException("Keys are empty!");
            }
        })) return;

        EntityId entityId = EntityIdFactory.getByTypeAndId(cmd.getEntityType(), cmd.getEntityId());
        List<String> keys = new ArrayList<>(getKeys(cmd).orElse(Collections.emptySet()));
        List<ReadTsKvQuery> queries = keys.stream().map(key -> new BaseReadTsKvQuery(key, cmd.getStartTs(), cmd.getEndTs(), cmd.getInterval(), getLimit(cmd.getLimit()), getAggregation(cmd.getAgg())))
                .collect(Collectors.toList());

        FutureCallback<List<TsKvEntry>> callback = new FutureCallback<List<TsKvEntry>>() {
            @Override
            public void onSuccess(List<TsKvEntry> data) {
                sendUpdate(sessionRef, new TelemetrySubscriptionUpdate(cmd.getCmdId(), data));
            }

            @Override
            public void onFailure(Throwable e) {
                TelemetrySubscriptionUpdate update;
                if (UnauthorizedException.class.isInstance(e)) {
                    update = new TelemetrySubscriptionUpdate(cmd.getCmdId(), SubscriptionErrorCode.UNAUTHORIZED,
                            SubscriptionErrorCode.UNAUTHORIZED.getDefaultMsg());
                } else {
                    update = new TelemetrySubscriptionUpdate(cmd.getCmdId(), SubscriptionErrorCode.INTERNAL_ERROR,
                            FAILED_TO_FETCH_DATA);
                }
                sendUpdate(sessionRef, update);
            }
        };
        accessValidator.validate(sessionRef.getSecurityCtx(), Operation.READ_TELEMETRY, entityId,
                on(r -> Futures.addCallback(tsService.findAll(sessionRef.getSecurityCtx().getTenantId(), entityId, queries), callback, executor), callback::onFailure));
    }

    private void handleWsAttributesSubscription(WebSocketSessionRef sessionRef,
                                                AttributesSubscriptionCmd cmd,
                                                String sessionId,
                                                EntityId entityId) {
        long queryTs = System.currentTimeMillis();
        FutureCallback<List<AttributeKvEntry>> callback = new FutureCallback<>() {
            @Override
            public void onSuccess(List<AttributeKvEntry> data) {
                List<TsKvEntry> attributesData = data.stream().map(d -> new BasicTsKvEntry(d.getLastUpdateTs(), d)).collect(Collectors.toList());

                Map<String, Long> subState = new HashMap<>(attributesData.size());
                attributesData.forEach(v -> subState.put(v.getKey(), v.getTs()));

                TbAttributeSubscriptionScope scope = StringUtils.isEmpty(cmd.getScope()) ? TbAttributeSubscriptionScope.ANY_SCOPE : TbAttributeSubscriptionScope.valueOf(cmd.getScope());

                Lock subLock = new ReentrantLock();
                TbAttributeSubscription sub = TbAttributeSubscription.builder()
                        .serviceId(serviceId)
                        .sessionId(sessionId)
                        .subscriptionId(registerNewSessionSubId(sessionId, sessionRef, cmd.getCmdId()))
                        .tenantId(sessionRef.getSecurityCtx().getTenantId())
                        .entityId(entityId)
                        .queryTs(queryTs)
                        .allKeys(true)
                        .keyStates(subState)
                        .updateProcessor((subscription, update) -> {
                            subLock.lock();
                            try {
                                sendUpdate(subscription.getSessionId(), cmd.getCmdId(), update);
                            } finally {
                                subLock.unlock();
                            }
                        })
                        .scope(scope)
                        .build();

                subLock.lock();
                try {
                    oldSubService.addSubscription(sub, sessionRef);
                    sendUpdate(sessionRef, new TelemetrySubscriptionUpdate(cmd.getCmdId(), attributesData));
                } finally {
                    subLock.unlock();
                }
            }

            @Override
            public void onFailure(Throwable e) {
                log.error(FAILED_TO_FETCH_ATTRIBUTES, e);
                sendError(sessionRef, cmd.getCmdId(), SubscriptionErrorCode.INTERNAL_ERROR, FAILED_TO_FETCH_ATTRIBUTES);
            }
        };


        if (StringUtils.isEmpty(cmd.getScope())) {
            accessValidator.validate(sessionRef.getSecurityCtx(), Operation.READ_ATTRIBUTES, entityId, getAttributesFetchCallback(sessionRef.getSecurityCtx().getTenantId(), entityId, callback));
        } else {
            accessValidator.validate(sessionRef.getSecurityCtx(), Operation.READ_ATTRIBUTES, entityId, getAttributesFetchCallback(sessionRef.getSecurityCtx().getTenantId(), entityId, cmd.getScope(), callback));
        }
    }

    /**
     * 处理时间序列订阅命令
     */
    private void handleWsTimeseriesSubscriptionCmd(WebSocketSessionRef sessionRef, TimeseriesSubscriptionCmd cmd) {
        if (!processSubscription(sessionRef, cmd)) {
            return;
        }

        String sessionId = sessionRef.getSessionId();
        if (cmd.isUnsubscribe()) {
            unsubscribe(sessionRef, cmd, sessionId);
        } else if (validateSubscriptionCmd(sessionRef, cmd)) {
            EntityId entityId = EntityIdFactory.getByTypeAndId(cmd.getEntityType(), cmd.getEntityId());
            Optional<Set<String>> keysOptional = getKeys(cmd);

            if (keysOptional.isPresent()) {
                handleWsTimeSeriesSubscriptionByKeys(sessionRef, cmd, sessionId, entityId);
            } else {
                handleWsTimeSeriesSubscription(sessionRef, cmd, sessionId, entityId);
            }
        }
    }

    private void handleWsTimeSeriesSubscriptionByKeys(WebSocketSessionRef sessionRef,
                                                      TimeseriesSubscriptionCmd cmd, String sessionId, EntityId entityId) {
        long startTs;
        long queryTs = System.currentTimeMillis();
        if (cmd.getTimeWindow() > 0) {
            List<String> keys = new ArrayList<>(getKeys(cmd).orElse(Collections.emptySet()));
            log.debug("[{}] fetching timeseries data for last {} ms for keys: ({}) for device : {}", sessionId, cmd.getTimeWindow(), cmd.getKeys(), entityId);
            startTs = cmd.getStartTs();
            long endTs = cmd.getStartTs() + cmd.getTimeWindow();
            List<ReadTsKvQuery> queries = keys.stream().map(key -> new BaseReadTsKvQuery(key, startTs, endTs, cmd.getInterval(),
                    getLimit(cmd.getLimit()), getAggregation(cmd.getAgg()))).collect(Collectors.toList());
            final FutureCallback<List<TsKvEntry>> callback = getSubscriptionCallback(sessionRef, cmd, sessionId, entityId, queryTs, startTs, keys);
            accessValidator.validate(sessionRef.getSecurityCtx(), Operation.READ_TELEMETRY, entityId,
                    on(r -> Futures.addCallback(tsService.findAll(sessionRef.getSecurityCtx().getTenantId(), entityId, queries), callback, executor), callback::onFailure));
        } else {
            List<String> keys = new ArrayList<>(getKeys(cmd).orElse(Collections.emptySet()));
            startTs = System.currentTimeMillis();
            log.debug("[{}] fetching latest timeseries data for keys: ({}) for device : {}", sessionId, cmd.getKeys(), entityId);
            final FutureCallback<List<TsKvEntry>> callback = getSubscriptionCallback(sessionRef, cmd, sessionId, entityId, queryTs, startTs, keys);
            accessValidator.validate(sessionRef.getSecurityCtx(), Operation.READ_TELEMETRY, entityId,
                    on(r -> Futures.addCallback(tsService.findLatest(sessionRef.getSecurityCtx().getTenantId(), entityId, keys), callback, executor), callback::onFailure));
        }
    }

    private void handleWsTimeSeriesSubscription(WebSocketSessionRef sessionRef,
                                                TimeseriesSubscriptionCmd cmd, String sessionId, EntityId entityId) {
        long queryTs = System.currentTimeMillis();
        FutureCallback<List<TsKvEntry>> callback = new FutureCallback<List<TsKvEntry>>() {
            @Override
            public void onSuccess(List<TsKvEntry> data) {
                Map<String, Long> subState = new HashMap<>(data.size());
                data.forEach(v -> subState.put(v.getKey(), v.getTs()));

                Lock subLock = new ReentrantLock();
                TbTimeSeriesSubscription sub = getTsSubscription(subState, subLock, sessionId, sessionRef, cmd, entityId, queryTs, true);

                subLock.lock();
                try {
                    oldSubService.addSubscription(sub, sessionRef);
                    sendUpdate(sessionRef, new TelemetrySubscriptionUpdate(cmd.getCmdId(), data));
                } finally {
                    subLock.unlock();
                }
            }

            @Override
            public void onFailure(Throwable e) {
                TelemetrySubscriptionUpdate update;
                if (UnauthorizedException.class.isInstance(e)) {
                    update = new TelemetrySubscriptionUpdate(cmd.getCmdId(), SubscriptionErrorCode.UNAUTHORIZED,
                            SubscriptionErrorCode.UNAUTHORIZED.getDefaultMsg());
                } else {
                    update = new TelemetrySubscriptionUpdate(cmd.getCmdId(), SubscriptionErrorCode.INTERNAL_ERROR,
                            FAILED_TO_FETCH_DATA);
                }
                sendUpdate(sessionRef, update);
            }
        };
        accessValidator.validate(sessionRef.getSecurityCtx(), Operation.READ_TELEMETRY, entityId,
                on(r -> Futures.addCallback(tsService.findAllLatest(sessionRef.getSecurityCtx().getTenantId(), entityId), callback, executor), callback::onFailure));
    }

    private TbTimeSeriesSubscription getTsSubscription(Map<String, Long> subState, Lock subLock, String sessionId, WebSocketSessionRef sessionRef, TimeseriesSubscriptionCmd cmd, EntityId entityId, long queryTs, boolean allKeys) {
        return TbTimeSeriesSubscription.builder()
                .serviceId(serviceId)
                .sessionId(sessionId)
                .subscriptionId(registerNewSessionSubId(sessionId, sessionRef, cmd.getCmdId()))
                .tenantId(sessionRef.getSecurityCtx().getTenantId())
                .entityId(entityId)
                .updateProcessor((subscription, update) -> {
                    subLock.lock();
                    try {
                        sendUpdate(subscription.getSessionId(), cmd.getCmdId(), update);
                    } finally {
                        subLock.unlock();
                    }
                })
                .queryTs(queryTs)
                .allKeys(allKeys)
                .keyStates(subState)
                .latestValues(LATEST_TELEMETRY_SCOPE.equals(cmd.getScope()))
                .build();
    }

    private FutureCallback<List<TsKvEntry>> getSubscriptionCallback(final WebSocketSessionRef sessionRef, final TimeseriesSubscriptionCmd cmd,
                                                                    final String sessionId, final EntityId entityId, final long queryTs, final long startTs, final List<String> keys) {
        return new FutureCallback<>() {
            @Override
            public void onSuccess(List<TsKvEntry> data) {
                Map<String, Long> subState = new HashMap<>(keys.size());
                keys.forEach(key -> subState.put(key, startTs));
                data.forEach(v -> subState.put(v.getKey(), v.getTs()));

                Lock subLock = new ReentrantLock();
                TbTimeSeriesSubscription sub = getTsSubscription(subState, subLock, sessionId, sessionRef, cmd, entityId, queryTs, false);

                subLock.lock();
                try {
                    oldSubService.addSubscription(sub, sessionRef);
                    sendUpdate(sessionRef, new TelemetrySubscriptionUpdate(cmd.getCmdId(), data));
                } finally {
                    subLock.unlock();
                }
            }

            @Override
            public void onFailure(Throwable e) {
                if (e instanceof TenantRateLimitException || e.getCause() instanceof TenantRateLimitException) {
                    log.trace("[{}] Tenant rate limit detected for subscription: [{}]:{}", sessionRef.getSecurityCtx().getTenantId(), entityId, cmd);
                } else {
                    log.info(FAILED_TO_FETCH_DATA, e);
                }
                sendError(sessionRef, cmd.getCmdId(), SubscriptionErrorCode.INTERNAL_ERROR, FAILED_TO_FETCH_DATA);
            }
        };
    }

    private void unsubscribe(WebSocketSessionRef sessionRef, SubscriptionCmd cmd, String sessionId) {
        TenantId tenantId = sessionRef.getSecurityCtx().getTenantId();
        if (cmd.getEntityId() == null || cmd.getEntityId().isEmpty()) {
            log.warn("[{}][{}][{}] Cleanup session due to empty entity id.", tenantId, sessionId, cmd.getCmdId());
            cleanupSessionById(tenantId, sessionId);
        } else {
            Integer subId = sessionCmdMap.getOrDefault(sessionId, Collections.emptyMap()).remove(cmd.getCmdId());
            if (subId == null) {
                log.trace("[{}][{}][{}] Failed to lookup subscription id mapping", tenantId, sessionId, cmd.getCmdId());
                subId = cmd.getCmdId();
            }
            oldSubService.cancelSubscription(tenantId, sessionId, subId);
        }
    }

    private void cleanupSessionById(TenantId tenantId, String sessionId) {
        wsSessionsMap.remove(sessionId);
        oldSubService.cancelAllSessionSubscriptions(tenantId, sessionId);
        sessionCmdMap.remove(sessionId);
        entityDataSubService.cancelAllSessionSubscriptions(sessionId);
    }

    private boolean validateSubscriptionCmd(WebSocketSessionRef sessionRef, EntityDataCmd cmd) {
        return validateCmd(sessionRef, cmd, () -> {
            if (cmd.getQuery() == null && !cmd.hasAnyCmd()) {
                throw new IllegalArgumentException("Query is empty!");
            }
        });
    }

    private boolean validateSubscriptionCmd(WebSocketSessionRef sessionRef, EntityCountCmd cmd) {
        return validateCmd(sessionRef, cmd, () -> {
            if (cmd.getQuery() == null) {
                throw new IllegalArgumentException("Query is empty!");
            }
        });
    }

    private boolean validateSubscriptionCmd(WebSocketSessionRef sessionRef, AlarmDataCmd cmd) {
        return validateCmd(sessionRef, cmd, () -> {
            if (cmd.getQuery() == null) {
                throw new IllegalArgumentException("Query is empty!");
            }
        });
    }

    private boolean validateSubscriptionCmd(WebSocketSessionRef sessionRef, SubscriptionCmd cmd) {
        return validateCmd(sessionRef, cmd, () -> {
            if (cmd.getEntityId() == null || cmd.getEntityId().isEmpty()) {
                throw new IllegalArgumentException("Device id is empty!");
            }
        });
    }

    private boolean validateSessionMetadata(WebSocketSessionRef sessionRef, int cmdId, String sessionId) {
        WsSessionMetaData sessionMD = wsSessionsMap.get(sessionId);
        if (sessionMD == null) {
            log.warn("[{}] Session meta data not found. ", sessionId);
            sendError(sessionRef, cmdId, SubscriptionErrorCode.INTERNAL_ERROR, SESSION_META_DATA_NOT_FOUND);
            return false;
        } else {
            return true;
        }
    }

    private boolean validateCmd(WebSocketSessionRef sessionRef, WsCmd cmd) {
        return validateCmd(sessionRef, cmd, null);
    }

    private <C extends WsCmd> boolean validateCmd(WebSocketSessionRef sessionRef, C cmd, Runnable validator) {
        if (cmd.getCmdId() < 0) {
            sendError(sessionRef, cmd.getCmdId(), SubscriptionErrorCode.BAD_REQUEST, "Cmd id is negative value!");
            return false;
        }
        try {
            if (validator != null) {
                validator.run();
            }
        } catch (Exception e) {
            sendError(sessionRef, cmd.getCmdId(), SubscriptionErrorCode.BAD_REQUEST, e.getMessage());
            return false;
        }
        return true;
    }

    private void sendUpdate(WebSocketSessionRef sessionRef, EntityDataUpdate update) {
        sendUpdate(sessionRef, update.getCmdId(), update);
    }

    private void sendUpdate(WebSocketSessionRef sessionRef, TelemetrySubscriptionUpdate update) {
        sendUpdate(sessionRef, update.getSubscriptionId(), update);
    }

    private void sendUpdate(WebSocketSessionRef sessionRef, int cmdId, Object update) {
        try {
            String msg = JacksonUtil.OBJECT_MAPPER.writeValueAsString(update);
            executor.submit(() -> {
                try {
                    msgEndpoint.send(sessionRef, cmdId, msg);
                } catch (IOException e) {
                    log.warn("[{}] Failed to send reply: {}", sessionRef.getSessionId(), update, e);
                }
            });
        } catch (JsonProcessingException e) {
            log.warn("[{}] Failed to encode reply: {}", sessionRef.getSessionId(), update, e);
        }
    }

    /**
     * 定期发送 Ping 消息保持连接
     */
    private void sendPing() {
        long currentTime = System.currentTimeMillis();
        wsSessionsMap.values().forEach(md ->
                executor.submit(() -> {
                    try {
                        msgEndpoint.sendPing(md.getSessionRef(), currentTime);
                    } catch (IOException e) {
                        log.warn("[{}] Failed to send ping:", md.getSessionRef().getSessionId(), e);
                    }
                }));
    }

    private static Optional<Set<String>> getKeys(TelemetryPluginCmd cmd) {
        if (!StringUtils.isEmpty(cmd.getKeys())) {
            Set<String> keys = new HashSet<>();
            Collections.addAll(keys, cmd.getKeys().split(","));
            return Optional.of(keys);
        } else {
            return Optional.empty();
        }
    }

    private ListenableFuture<List<AttributeKvEntry>> mergeAllAttributesFutures(List<ListenableFuture<List<AttributeKvEntry>>> futures) {
        return Futures.transform(Futures.successfulAsList(futures),
                (Function<? super List<List<AttributeKvEntry>>, ? extends List<AttributeKvEntry>>) input -> {
                    List<AttributeKvEntry> tmp = new ArrayList<>();
                    if (input != null) {
                        input.forEach(tmp::addAll);
                    }
                    return tmp;
                }, executor);
    }

    private <T> FutureCallback<ValidationResult> getAttributesFetchCallback(final TenantId tenantId, final EntityId entityId, final List<String> keys, final FutureCallback<List<AttributeKvEntry>> callback) {
        return new FutureCallback<ValidationResult>() {
            @Override
            public void onSuccess(@Nullable ValidationResult result) {
                List<ListenableFuture<List<AttributeKvEntry>>> futures = new ArrayList<>();
                for (AttributeScope scope : AttributeScope.values()) {
                    futures.add(attributesService.find(tenantId, entityId, scope, keys));
                }

                ListenableFuture<List<AttributeKvEntry>> future = mergeAllAttributesFutures(futures);
                Futures.addCallback(future, callback, MoreExecutors.directExecutor());
            }

            @Override
            public void onFailure(Throwable t) {
                callback.onFailure(t);
            }
        };
    }

    private <T> FutureCallback<ValidationResult> getAttributesFetchCallback(final TenantId tenantId, final EntityId entityId, final String scope, final List<String> keys, final FutureCallback<List<AttributeKvEntry>> callback) {
        return new FutureCallback<ValidationResult>() {
            @Override
            public void onSuccess(@Nullable ValidationResult result) {
                Futures.addCallback(attributesService.find(tenantId, entityId, AttributeScope.valueOf(scope), keys), callback, MoreExecutors.directExecutor());
            }

            @Override
            public void onFailure(Throwable t) {
                callback.onFailure(t);
            }
        };
    }

    private <T> FutureCallback<ValidationResult> getAttributesFetchCallback(final TenantId tenantId, final EntityId entityId, final FutureCallback<List<AttributeKvEntry>> callback) {
        return new FutureCallback<ValidationResult>() {
            @Override
            public void onSuccess(@Nullable ValidationResult result) {
                List<ListenableFuture<List<AttributeKvEntry>>> futures = new ArrayList<>();
                for (AttributeScope scope : AttributeScope.values()) {
                    futures.add(attributesService.findAll(tenantId, entityId, scope));
                }

                ListenableFuture<List<AttributeKvEntry>> future = mergeAllAttributesFutures(futures);
                Futures.addCallback(future, callback, MoreExecutors.directExecutor());
            }

            @Override
            public void onFailure(Throwable t) {
                callback.onFailure(t);
            }
        };
    }

    private <T> FutureCallback<ValidationResult> getAttributesFetchCallback(final TenantId tenantId, final EntityId entityId, final String scope, final FutureCallback<List<AttributeKvEntry>> callback) {
        return new FutureCallback<ValidationResult>() {
            @Override
            public void onSuccess(@Nullable ValidationResult result) {
                Futures.addCallback(attributesService.findAll(tenantId, entityId, AttributeScope.valueOf(scope)), callback, MoreExecutors.directExecutor());
            }

            @Override
            public void onFailure(Throwable t) {
                callback.onFailure(t);
            }
        };
    }

    private FutureCallback<ValidationResult> on(Consumer<Void> success, Consumer<Throwable> failure) {
        return new FutureCallback<ValidationResult>() {
            @Override
            public void onSuccess(@Nullable ValidationResult result) {
                ValidationResultCode resultCode = result.getResultCode();
                if (resultCode == ValidationResultCode.OK) {
                    success.accept(null);
                } else {
                    onFailure(ValidationCallback.getException(result));
                }
            }

            @Override
            public void onFailure(Throwable t) {
                failure.accept(t);
            }
        };
    }


    public static Aggregation getAggregation(String agg) {
        return StringUtils.isEmpty(agg) ? DEFAULT_AGGREGATION : Aggregation.valueOf(agg);
    }

    private int getLimit(int limit) {
        return limit == 0 ? DEFAULT_LIMIT : limit;
    }

    private DefaultTenantProfileConfiguration getTenantProfileConfiguration(WebSocketSessionRef sessionRef) {
        return Optional.ofNullable(tenantProfileCache.get(sessionRef.getSecurityCtx().getTenantId()))
                .map(TenantProfile::getDefaultProfileConfiguration).orElse(null);
    }

    public static <C extends WsCmd> WsCmdHandler<C> newCmdHandler(BiConsumer<WebSocketSessionRef, C> handler) {
        return new WsCmdHandler<>(handler);
    }

    @RequiredArgsConstructor
    @Getter
    @SuppressWarnings("unchecked")
    public static class WsCmdHandler<C extends WsCmd> {
        protected final BiConsumer<WebSocketSessionRef, C> handler;

        public void handle(WebSocketSessionRef sessionRef, WsCmd cmd) {
            handler.accept(sessionRef, (C) cmd);
        }
    }

}
