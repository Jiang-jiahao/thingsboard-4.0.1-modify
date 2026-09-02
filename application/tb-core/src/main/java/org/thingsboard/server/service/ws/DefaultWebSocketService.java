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
 * {@link WebSocketService} 的默认实现，是 ThingsBoard Core 面向 UI 的实时 WebSocket 通道中枢。
 *
 * <p>本类不直接持有底层 WebSocket 连接，而是接收 {@link org.thingsboard.server.controller.plugin.TbWebSocketHandler} 转来的会话事件与命令，
 * 完成权限校验、订阅创建/取消、配额控制，再把更新编码后经 {@link WebSocketMsgEndpoint} 推回前端。
 *
 * <h2>职责拆分</h2>
 * <ol>
 *   <li><b>会话生命周期</b>：连接建立时登记 {@link WsSessionMetaData}；关闭或底层已断开时清理全部订阅与配额计数。</li>
 *   <li><b>命令路由</b>：按 {@link WsCmdType} 把客户端命令分发给对应处理器。v1 属性/遥测/历史由本类处理；
 *       v2 实体数据、告警、计数交给 {@link TbEntityDataSubscriptionService}；通知类命令交给 {@link NotificationCommandsHandler}。</li>
 *   <li><b>订阅与下行</b>：v1 订阅注册到 {@link TbLocalSubscriptionService}，后续属性/遥测变更通过回调推送；
 *       发送前将 JSON 序列化并提交到独立线程池，避免阻塞命令处理线程。</li>
 *   <li><b>安全与配额</b>：读属性/遥测前经 {@link AccessValidator} 校验；按租户画像限制租户、客户、普通用户、公共用户的并发订阅数，超限则关闭会话。</li>
 *   <li><b>保活</b>：定时向所有活跃会话发送 Ping，超时由端点侧判定并关闭。</li>
 * </ol>
 *
 * <h2>cmdId 与 subscriptionId</h2>
 * 客户端使用 {@code cmdId} 标识一条命令。本类为每条 v1 订阅再分配内部 {@code subscriptionId}，
 * 并写入 {@code sessionCmdMap}。取消订阅时先用 cmdId 反查内部 id，再通知本地订阅服务。
 * 下行旧版遥测更新时，会把内部 subscriptionId 替换回 cmdId，保证前端仍按自己的命令 id 接收。
 *
 * @see WebSocketService
 * @see org.thingsboard.server.controller.plugin.TbWebSocketHandler
 */
@Service
@TbCoreComponent
@Slf4j
@RequiredArgsConstructor
public class DefaultWebSocketService implements WebSocketService {

    /**
     * 一个 Ping 周期内允许的最大尝试次数。定时任务间隔 = {@code pingTimeout / NUMBER_OF_PING_ATTEMPTS}，
     * 这样在完整超时窗口内会连续探测多次，避免偶发丢包立刻判死。
     */
    public static final int NUMBER_OF_PING_ATTEMPTS = 3;

    /** 历史/窗口查询未指定 limit 时的默认条数上限。 */
    private static final int DEFAULT_LIMIT = 100;

    /** 查询未指定聚合类型时的默认值：不做聚合，原样返回采样点。 */
    private static final Aggregation DEFAULT_AGGREGATION = Aggregation.NONE;

    /**
     * 会话元数据校验失败时使用的占位订阅 id。此时还没有合法的 cmdId 可回传，
     * 但仍需构造一条错误更新通知前端。
     */
    private static final int UNKNOWN_SUBSCRIPTION_ID = 0;

    private static final String PROCESSING_MSG = "[{}] Processing: {}";
    private static final String FAILED_TO_FETCH_DATA = "Failed to fetch data!";
    private static final String FAILED_TO_FETCH_ATTRIBUTES = "Failed to fetch attributes!";
    private static final String SESSION_META_DATA_NOT_FOUND = "Session meta-data not found!";

    /**
     * 当前节点上所有活跃 WebSocket 会话。key 为 sessionId，value 为会话引用及其最近 Ping 等元数据。
     * 下行推送、Ping、主动关闭都先查这张表；会话关闭后必须移除，否则会向已断开连接继续发包。
     */
    private final ConcurrentMap<String, WsSessionMetaData> wsSessionsMap = new ConcurrentHashMap<>();

    /**
     * v1 本地订阅服务。属性订阅、时间序列订阅的注册/取消以及会话级批量取消都走这里。
     * 实体数据 v2 订阅不经过本服务。
     */
    private final TbLocalSubscriptionService oldSubService;

    /**
     * v2 实体数据订阅服务。处理实体列表/时序、告警列表、实体计数、告警计数、告警状态及其取消订阅。
     */
    private final TbEntityDataSubscriptionService entityDataSubService;

    /**
     * 通知中心相关命令：未读列表、未读计数、标记已读、全部已读、取消订阅。
     */
    private final NotificationCommandsHandler notificationCmdsHandler;

    /**
     * 真正往 WebSocket 连接写数据的端点。本类只负责编码与调度，发送、Ping、关闭、连接是否仍打开均委托给它。
     */
    private final WebSocketMsgEndpoint msgEndpoint;

    /**
     * 实体级权限校验器。创建属性/遥测订阅或查询历史前，必须先通过 READ_ATTRIBUTES / READ_TELEMETRY 校验。
     */
    private final AccessValidator accessValidator;

    /** 属性存储访问，用于订阅前拉取当前属性快照。 */
    private final AttributesService attributesService;

    /** 时间序列存储访问，用于订阅前拉取最新值或时间窗口内的历史点。 */
    private final TimeseriesService tsService;

    /** 提供当前 Core 节点的 serviceId，写入订阅对象，便于集群内识别订阅归属。 */
    private final TbServiceInfoProvider serviceInfoProvider;

    /** 租户画像缓存。配额开关与上限（每租户/客户/用户订阅数）都从画像配置读取。 */
    private final TbTenantProfileCache tenantProfileCache;

    /**
     * WebSocket Ping 总超时（毫秒），默认 30 秒。实际调度间隔为该值除以 {@link #NUMBER_OF_PING_ATTEMPTS}。
     */
    @Value("${server.ws.ping_timeout:30000}")
    private long pingTimeout;

    /**
     * 租户级并发订阅计数。value 中的元素格式为 {@code [sessionId]:[cmdId]}。
     * 仅当画像中 {@code maxWsSubscriptionsPerTenant > 0} 时才启用。
     */
    private final ConcurrentMap<TenantId, Set<String>> tenantSubscriptionsMap = new ConcurrentHashMap<>();

    /**
     * 客户级并发订阅计数。仅客户用户且画像开启 {@code maxWsSubscriptionsPerCustomer} 时使用。
     */
    private final ConcurrentMap<CustomerId, Set<String>> customerSubscriptionsMap = new ConcurrentHashMap<>();

    /**
     * 普通登录用户（USER_NAME）的并发订阅计数。仅客户侧用户且画像开启对应上限时使用。
     */
    private final ConcurrentMap<UserId, Set<String>> regularUserSubscriptionsMap = new ConcurrentHashMap<>();

    /**
     * 公共用户（PUBLIC_ID）的并发订阅计数。Dashboard 公开链接场景使用。
     */
    private final ConcurrentMap<UserId, Set<String>> publicUserSubscriptionsMap = new ConcurrentHashMap<>();

    /**
     * 会话内「客户端 cmdId → 内部 subscriptionId」映射。
     * v1 订阅注册时写入；取消订阅时用 cmdId 反查内部 id；会话清理时整表删除。
     */
    private final ConcurrentMap<String, Map<Integer, Integer>> sessionCmdMap = new ConcurrentHashMap<>();

    /**
     * 命令处理与下行发送共用的 work-stealing 线程池。
     * 属性/遥测查询回调、JSON 发送、Ping 提交都在此执行，避免占用 WebSocket I/O 线程。
     */
    private ExecutorService executor;

    /** 单线程定时器，按固定间隔遍历所有会话发送 Ping。 */
    private ScheduledExecutorService pingExecutor;

    /** 当前 Core 节点标识，写入每条 v1 订阅，供订阅服务在集群中定位归属节点。 */
    private String serviceId;

    /**
     * 命令类型到处理器的注册表。{@link #init()} 时填充，{@link #handleCommands} 按 {@link WsCmdType} 查找。
     * 未注册的类型会被静默忽略。
     */
    private Map<WsCmdType, WsCmdHandler<? extends WsCmd>> cmdsHandlers;

    /**
     * 启动阶段初始化：读取本节点 serviceId、创建线程池、启动 Ping 定时任务，并注册全部命令处理器。
     *
     * <p>处理器分四组：
     * <ul>
     *   <li>v1 遥测：属性订阅、时序订阅、历史查询；</li>
     *   <li>v2 实体/告警数据：实体数据、告警数据、实体计数、告警计数、告警状态；</li>
     *   <li>v2 取消订阅：上述数据类命令对应的 unsubscribe；</li>
     *   <li>通知：未读列表/计数、标记已读、取消订阅。</li>
     * </ul>
     */
    @PostConstruct
    public void init() {
        serviceId = serviceInfoProvider.getServiceId();
        executor = ThingsBoardExecutors.newWorkStealingPool(50, getClass());

        pingExecutor = ThingsBoardExecutors.newSingleThreadScheduledExecutor("telemetry-web-socket-ping");
        pingExecutor.scheduleWithFixedDelay(this::sendPing, pingTimeout / NUMBER_OF_PING_ATTEMPTS, pingTimeout / NUMBER_OF_PING_ATTEMPTS, TimeUnit.MILLISECONDS);

        cmdsHandlers = new EnumMap<>(WsCmdType.class);

        cmdsHandlers.put(WsCmdType.ATTRIBUTES, newCmdHandler(this::handleWsAttributesSubscriptionCmd));
        cmdsHandlers.put(WsCmdType.TIMESERIES, newCmdHandler(this::handleWsTimeseriesSubscriptionCmd));
        cmdsHandlers.put(WsCmdType.TIMESERIES_HISTORY, newCmdHandler(this::handleWsHistoryCmd));

        cmdsHandlers.put(WsCmdType.ENTITY_DATA, newCmdHandler(this::handleWsEntityDataCmd));
        cmdsHandlers.put(WsCmdType.ALARM_DATA, newCmdHandler(this::handleWsAlarmDataCmd));
        cmdsHandlers.put(WsCmdType.ENTITY_COUNT, newCmdHandler(this::handleWsEntityCountCmd));
        cmdsHandlers.put(WsCmdType.ALARM_COUNT, newCmdHandler(this::handleWsAlarmCountCmd));
        cmdsHandlers.put(WsCmdType.ALARM_STATUS, newCmdHandler(this::handleWsAlarmsStatusCmd));

        cmdsHandlers.put(WsCmdType.ENTITY_DATA_UNSUBSCRIBE, newCmdHandler(this::handleWsDataUnsubscribeCmd));
        cmdsHandlers.put(WsCmdType.ALARM_DATA_UNSUBSCRIBE, newCmdHandler(this::handleWsDataUnsubscribeCmd));
        cmdsHandlers.put(WsCmdType.ENTITY_COUNT_UNSUBSCRIBE, newCmdHandler(this::handleWsDataUnsubscribeCmd));
        cmdsHandlers.put(WsCmdType.ALARM_COUNT_UNSUBSCRIBE, newCmdHandler(this::handleWsDataUnsubscribeCmd));
        cmdsHandlers.put(WsCmdType.ALARM_STATUS_UNSUBSCRIBE, newCmdHandler(this::handleWsDataUnsubscribeCmd));

        cmdsHandlers.put(WsCmdType.NOTIFICATIONS, newCmdHandler(notificationCmdsHandler::handleUnreadNotificationsSubCmd));
        cmdsHandlers.put(WsCmdType.NOTIFICATIONS_COUNT, newCmdHandler(notificationCmdsHandler::handleUnreadNotificationsCountSubCmd));
        cmdsHandlers.put(WsCmdType.MARK_NOTIFICATIONS_AS_READ, newCmdHandler(notificationCmdsHandler::handleMarkAsReadCmd));
        cmdsHandlers.put(WsCmdType.MARK_ALL_NOTIFICATIONS_AS_READ, newCmdHandler(notificationCmdsHandler::handleMarkAllAsReadCmd));
        cmdsHandlers.put(WsCmdType.NOTIFICATIONS_UNSUBSCRIBE, newCmdHandler(notificationCmdsHandler::handleUnsubCmd));
    }

    /**
     * 节点停机时立即关闭 Ping 定时器与命令处理线程池，不再接受新任务。
     * 进行中的发送可能被中断，会话清理依赖连接关闭事件或上层调用 {@link #cleanupIfStale}。
     */
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
     * 处理底层连接层上报的会话事件。
     *
     * <ul>
     *   <li>{@code ESTABLISHED}：登记会话元数据，之后才允许处理命令；</li>
     *   <li>{@code ERROR}：仅记录日志，不断开（断开由端点或后续 CLOSED 处理）；</li>
     *   <li>{@code CLOSED}：取消该会话全部 v1/v2 订阅，并从各层配额集合中移除本会话的订阅记录。</li>
     * </ul>
     *
     * @param sessionRef 当前会话引用（含 sessionId 与安全上下文）
     * @param event      会话事件（建立 / 错误 / 关闭）
     */
    @Override
    public void handleSessionEvent(WebSocketSessionRef sessionRef, SessionEvent event) {
        String sessionId = sessionRef.getSessionId();
        TenantId tenantId = sessionRef.getSecurityCtx().getTenantId();
        log.debug(PROCESSING_MSG, sessionId, event);
        switch (event.getEventType()) {
            case ESTABLISHED:
                wsSessionsMap.put(sessionId, new WsSessionMetaData(sessionRef));
                break;
            case ERROR:
                log.debug("[{}][{}] Unknown websocket session error: ", tenantId, sessionId,
                        event.getError().orElse(new RuntimeException("No error specified")));
                break;
            case CLOSED:
                cleanupSessionById(tenantId, sessionId);
                processSessionClose(sessionRef);
                break;
        }
    }

    /**
     * 处理客户端一次上行消息中的命令列表。空包装或空列表直接返回。
     *
     * <p>先校验会话元数据仍存在（防止连接已关但命令还在排队），再按类型逐条分发。
     * 单条命令失败不会中断后续命令：限流异常只打 debug；其它异常向该 cmdId 回 INTERNAL_ERROR。
     *
     * @param sessionRef      当前会话
     * @param commandsWrapper 客户端解析后的命令包装，内含多条 {@link WsCmd}
     */
    @Override
    public void handleCommands(WebSocketSessionRef sessionRef, WsCommandsWrapper commandsWrapper) {
        if (commandsWrapper == null || CollectionUtils.isEmpty(commandsWrapper.getCmds())) {
            return;
        }
        String sessionId = sessionRef.getSessionId();
        if (!validateSessionMetadata(sessionRef, UNKNOWN_SUBSCRIPTION_ID, sessionId)) {
            return;
        }

        for (WsCmd cmd : commandsWrapper.getCmds()) {
            log.debug("[{}][{}][{}] Processing cmd: {}", sessionId, cmd.getType(), cmd.getCmdId(), cmd);
            try {
                Optional.ofNullable(cmdsHandlers.get(cmd.getType()))
                        .ifPresent(cmdHandler -> cmdHandler.handle(sessionRef, cmd));
            } catch (TbRateLimitsException e) {
                log.debug("{} Failed to handle WS cmd: {}", sessionRef, cmd, e);
            } catch (Exception e) {
                sendError(sessionRef, cmd.getCmdId(), SubscriptionErrorCode.INTERNAL_ERROR, e.getMessage());
                log.error("{} Failed to handle WS cmd: {}", sessionRef, cmd, e);
            }
        }
    }

    /**
     * 处理 v2 实体数据订阅/查询命令（Dashboard 实体表、时序图等）。
     * 校验 query 或子命令非空后转给实体数据订阅服务。
     */
    private void handleWsEntityDataCmd(WebSocketSessionRef sessionRef, EntityDataCmd cmd) {
        if (validateSubscriptionCmd(sessionRef, cmd)) {
            entityDataSubService.handleCmd(sessionRef, cmd);
        }
    }

    /**
     * 处理 v2 实体计数订阅。要求 query 非空。
     */
    private void handleWsEntityCountCmd(WebSocketSessionRef sessionRef, EntityCountCmd cmd) {
        if (validateSubscriptionCmd(sessionRef, cmd)) {
            entityDataSubService.handleCmd(sessionRef, cmd);
        }
    }

    /**
     * 处理 v2 告警数据订阅（告警表）。要求 query 非空。
     */
    private void handleWsAlarmDataCmd(WebSocketSessionRef sessionRef, AlarmDataCmd cmd) {
        if (validateSubscriptionCmd(sessionRef, cmd)) {
            entityDataSubService.handleCmd(sessionRef, cmd);
        }
    }

    /**
     * 处理 v2 数据类取消订阅（实体数据/告警数据/计数/告警状态共用）。
     * 不经过本类的配额 {@link #processSubscription}，由实体数据订阅服务按 sessionId + cmd 取消。
     */
    private void handleWsDataUnsubscribeCmd(WebSocketSessionRef sessionRef, UnsubscribeCmd cmd) {
        entityDataSubService.cancelSubscription(sessionRef.getSessionId(), cmd);
    }

    /**
     * 处理 v2 告警计数订阅。只做通用 cmdId 校验，不强制 query。
     */
    private void handleWsAlarmCountCmd(WebSocketSessionRef sessionRef, AlarmCountCmd cmd) {
        if (validateCmd(sessionRef, cmd)) {
            entityDataSubService.handleCmd(sessionRef, cmd);
        }
    }

    /**
     * 处理 v2 告警状态订阅（某实体当前是否有活动告警等）。
     */
    private void handleWsAlarmsStatusCmd(WebSocketSessionRef sessionRef, AlarmStatusCmd cmd) {
        if (validateCmd(sessionRef, cmd)) {
            entityDataSubService.handleCmd(sessionRef, cmd);
        }
    }

    /**
     * 向指定会话推送 v1 遥测/属性订阅更新。
     *
     * <p>本地订阅服务内部使用 subscriptionId，前端只认识 cmdId。
     * 因此发送前把更新里的 subscriptionId 替换为 cmdId，避免前端对不上命令。
     *
     * @param sessionId 目标会话
     * @param cmdId     客户端命令 id
     * @param update    遥测订阅更新（键值、错误码等）
     */
    @Override
    public void sendUpdate(String sessionId, int cmdId, TelemetrySubscriptionUpdate update) {
        doSendUpdate(sessionId, cmdId, update.copyWithNewSubscriptionId(cmdId));
    }

    /**
     * 向指定会话推送 v2 命令更新（实体数据增量、告警刷新等）。cmdId 取自更新对象自身。
     */
    @Override
    public void sendUpdate(String sessionId, CmdUpdate update) {
        doSendUpdate(sessionId, update.getCmdId(), update);
    }

    /**
     * 构造一条错误更新并下发给当前会话。用于权限失败、参数非法、内部异常等场景。
     *
     * @param sessionRef 当前会话
     * @param subId      对应的 cmdId / 订阅 id
     * @param errorCode  协议层错误码
     * @param errorMsg   可读错误信息
     */
    @Override
    public void sendError(WebSocketSessionRef sessionRef, int subId, SubscriptionErrorCode errorCode, String errorMsg) {
        TelemetrySubscriptionUpdate update = new TelemetrySubscriptionUpdate(subId, errorCode, errorMsg);
        sendUpdate(sessionRef, update);
    }

    /**
     * 按 sessionId 查找元数据后再发送。会话已清理则丢弃，避免向已关闭连接写数据。
     */
    private <T> void doSendUpdate(String sessionId, int cmdId, T update) {
        WsSessionMetaData md = wsSessionsMap.get(sessionId);
        if (md != null) {
            sendUpdate(md.getSessionRef(), cmdId, update);
        }
    }

    /**
     * 主动关闭指定会话的底层连接。会话不存在则忽略。
     * 配额超限时会调用此路径，并带上 {@link CloseStatus#POLICY_VIOLATION}。
     */
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

    /**
     * 当发现底层连接已经断开、但本类仍保留会话记录时做补偿清理。
     * 典型场景：对端异常掉线，CLOSED 事件未及时到达，Ping 或其它探测发现连接已关。
     */
    @Override
    public void cleanupIfStale(TenantId tenantId, String sessionId) {
        if (!msgEndpoint.isOpen(sessionId)) {
            log.info("[{}] Cleaning up stale session ", sessionId);
            cleanupSessionById(tenantId, sessionId);
        }
    }

    /**
     * 会话关闭后的配额回收：从租户/客户/普通用户/公共用户四张计数表中，
     * 删除所有以当前 {@code [sessionId]} 为前缀的订阅记录。
     *
     * <p>只在对应画像上限大于 0（即该层配额已启用）时操作。客户用户才会检查客户与用户两级配额。
     */
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

    /**
     * 为一条 v1 订阅命令占用或释放配额。
     *
     * <p>订阅标识格式 {@code [sessionId]:[cmdId]}。unsubscribe 时从已启用的层级集合中移除；
     * 新建订阅时若任一已启用层级已达上限，则关闭会话并返回 {@code false}，调用方应立刻中止后续逻辑。
     * 画像缺失或对应上限为 0 表示该层不限流。
     *
     * @return {@code true} 表示可以继续创建/取消订阅；{@code false} 表示已超限或关闭会话失败
     */
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
     * 处理 v1 属性订阅命令。
     *
     * <p>流程：先占配额 → unsubscribe 则取消 → 否则校验实体 id → 按 keys 是否为空分流：
     * 指定键走 {@link #handleWsAttributesSubscriptionByKeys}，空 keys 表示订阅该实体全部属性。
     */
    private void handleWsAttributesSubscriptionCmd(WebSocketSessionRef sessionRef, AttributesSubscriptionCmd cmd) {
        if (!processSubscription(sessionRef, cmd)) {
            return;
        }

        String sessionId = sessionRef.getSessionId();
        if (cmd.isUnsubscribe()) {
            unsubscribe(sessionRef, cmd, sessionId);
        } else if (validateSubscriptionCmd(sessionRef, cmd)) {
            EntityId entityId = EntityIdFactory.getByTypeAndId(cmd.getEntityType(), cmd.getEntityId());
            log.debug("[{}] fetching latest attributes ({}) values for device: {}", sessionId, cmd.getKeys(), entityId);
            Optional<Set<String>> keysOptional = getKeys(cmd);
            if (keysOptional.isPresent()) {
                List<String> keys = new ArrayList<>(keysOptional.get());
                handleWsAttributesSubscriptionByKeys(sessionRef, cmd, sessionId, entityId, keys);
            } else {
                handleWsAttributesSubscription(sessionRef, cmd, sessionId, entityId);
            }
        }
    }

    /**
     * 按指定属性键创建订阅：先校验读权限并拉取当前值，再注册 {@link TbAttributeSubscription}，
     * 最后把快照作为第一条更新发给客户端。
     *
     * <p>{@code keyStates} 记录每个键上次推送的时间戳，供订阅服务做增量去重。
     * {@code scope} 为空时订阅任意范围（CLIENT/SERVER/SHARED），否则只订指定范围。
     * {@code subLock} 保证「注册订阅」与「推送快照」以及后续回调推送互斥，避免快照与增量乱序。
     */
    private void handleWsAttributesSubscriptionByKeys(WebSocketSessionRef sessionRef,
                                                      AttributesSubscriptionCmd cmd, String sessionId, EntityId entityId,
                                                      List<String> keys) {
        // 订阅创建时刻。本地订阅服务用它判断「查询之后」才到达的更新要不要推，避免把查库前的旧变更再推一遍。
        long queryTs = System.currentTimeMillis();
        FutureCallback<List<AttributeKvEntry>> callback = new FutureCallback<>() {
            @Override
            public void onSuccess(List<AttributeKvEntry> data) {
                // 属性条目转成与遥测更新相同的 TsKvEntry，方便统一走 TelemetrySubscriptionUpdate 下发。
                List<TsKvEntry> attributesData = data.stream().map(d -> new BasicTsKvEntry(d.getLastUpdateTs(), d)).collect(Collectors.toList());

                // keyStates：每个键上次已知的时间戳，订阅服务只推 ts 更新的点。
                // 先给请求里的每个 key 填 0（库里没有该键时也能订上）；再用查到的实际 ts 覆盖。
                Map<String, Long> subState = new HashMap<>(keys.size());
                keys.forEach(key -> subState.put(key, 0L));
                attributesData.forEach(v -> subState.put(v.getKey(), v.getTs()));

                // 未指定 scope 则 CLIENT/SERVER/SHARED 都订；否则只订命令里的那一类。
                TbAttributeSubscriptionScope scope = StringUtils.isEmpty(cmd.getScope()) ? TbAttributeSubscriptionScope.ANY_SCOPE : TbAttributeSubscriptionScope.valueOf(cmd.getScope());

                // 同一把锁包住：注册订阅、推快照、以及之后的增量回调。
                // 否则 addSubscription 成功后立刻来一条增量，可能插在快照前面，前端先看到新值再看到旧快照。
                Lock subLock = new ReentrantLock();
                TbAttributeSubscription sub = TbAttributeSubscription.builder()
                        .serviceId(serviceId)
                        .sessionId(sessionId)
                        // 给这条 cmd 发内部 subscriptionId，并记入 sessionCmdMap，取消时用 cmdId 反查。
                        .subscriptionId(registerNewSessionSubId(sessionId, sessionRef, cmd.getCmdId()))
                        .tenantId(sessionRef.getSecurityCtx().getTenantId())
                        .entityId(entityId)
                        .queryTs(queryTs)
                        .allKeys(false) // 只盯 keys 里列出的键，实体上新出现的其它属性不推
                        .keyStates(subState)
                        .scope(scope)
                        .updateProcessor((subscription, update) -> {
                            subLock.lock();
                            try {
                                // 下行时把内部 subscriptionId 换成前端的 cmdId（sendUpdate 重载里会 copy）。
                                sendUpdate(subscription.getSessionId(), cmd.getCmdId(), update);
                            } finally {
                                subLock.unlock();
                            }
                        })
                        .build();

                subLock.lock();
                try {
                    oldSubService.addSubscription(sub, sessionRef);
                    // 先把当前快照推给前端，之后的变更走上面的 updateProcessor。
                    sendUpdate(sessionRef, new TelemetrySubscriptionUpdate(cmd.getCmdId(), attributesData));
                } finally {
                    subLock.unlock();
                }

            }

            @Override
            public void onFailure(Throwable e) {
                // 查属性失败：无权限回 UNAUTHORIZED，其它回 INTERNAL_ERROR。不注册订阅。
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

        // 先鉴权 READ_ATTRIBUTES，通过后再按 keys 拉当前值，结果进上面的 callback。
        // 无 scope：三个 AttributeScope 都查再合并；有 scope：只查那一类。
        if (StringUtils.isEmpty(cmd.getScope())) {
            accessValidator.validate(sessionRef.getSecurityCtx(), Operation.READ_ATTRIBUTES, entityId, getAttributesFetchCallback(sessionRef.getSecurityCtx().getTenantId(), entityId, keys, callback));
        } else {
            accessValidator.validate(sessionRef.getSecurityCtx(), Operation.READ_ATTRIBUTES, entityId, getAttributesFetchCallback(sessionRef.getSecurityCtx().getTenantId(), entityId, cmd.getScope(), keys, callback));
        }
    }

    /**
     * 为当前会话的一条客户端命令分配内部订阅 id，并写入 {@link #sessionCmdMap}。
     * 内部 id 来自会话上的递增序列，保证同一会话内唯一，供本地订阅服务索引。
     *
     * @return 新的内部 subscriptionId
     */
    private int registerNewSessionSubId(String sessionId, WebSocketSessionRef sessionRef, int cmdId) {
        var cmdMap = sessionCmdMap.computeIfAbsent(sessionId, id -> new ConcurrentHashMap<>());
        var subId = sessionRef.getSessionSubIdSeq().incrementAndGet();
        cmdMap.put(cmdId, subId);
        return subId;
    }

    /**
     * 处理一次性历史时序查询（v1 TIMESERIES_HISTORY），不创建长期订阅。
     *
     * <p>要求实体 id 与 keys 均非空；按 startTs/endTs/interval/limit/agg 构造 {@link ReadTsKvQuery}，
     * 权限通过后调用 {@link TimeseriesService#findAll}，结果作为单次更新返回。
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

    /**
     * 订阅实体全部属性（命令未带 keys）。权限通过后按 scope 拉取全部属性快照，
     * {@code allKeys=true} 表示后续任意新属性键也会推送。
     */
    private void handleWsAttributesSubscription(WebSocketSessionRef sessionRef,
                                                AttributesSubscriptionCmd cmd,
                                                String sessionId,
                                                EntityId entityId) {
        // 订阅创建时刻，用途同 ByKeys：过滤查询开始前就已经存在的变更。
        long queryTs = System.currentTimeMillis();
        FutureCallback<List<AttributeKvEntry>> callback = new FutureCallback<>() {
            @Override
            public void onSuccess(List<AttributeKvEntry> data) {
                List<TsKvEntry> attributesData = data.stream().map(d -> new BasicTsKvEntry(d.getLastUpdateTs(), d)).collect(Collectors.toList());

                // 没有指定 keys：keyStates 只来自当前库里已有的属性。
                // allKeys=true 时，之后新出现的键也会推，那些新键不在这个 map 里，订阅服务按「新键」处理。
                Map<String, Long> subState = new HashMap<>(attributesData.size());
                attributesData.forEach(v -> subState.put(v.getKey(), v.getTs()));

                TbAttributeSubscriptionScope scope = StringUtils.isEmpty(cmd.getScope()) ? TbAttributeSubscriptionScope.ANY_SCOPE : TbAttributeSubscriptionScope.valueOf(cmd.getScope());

                // 锁的作用同 ByKeys：注册 + 推快照 与增量回调互斥，避免乱序。
                Lock subLock = new ReentrantLock();
                TbAttributeSubscription sub = TbAttributeSubscription.builder()
                        .serviceId(serviceId)
                        .sessionId(sessionId)
                        .subscriptionId(registerNewSessionSubId(sessionId, sessionRef, cmd.getCmdId()))
                        .tenantId(sessionRef.getSecurityCtx().getTenantId())
                        .entityId(entityId)
                        .queryTs(queryTs)
                        .allKeys(true) // 命令没带 keys：当前全部属性 + 以后新键都推
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
                // 与 ByKeys 不同：这里不区分 Unauthorized，一律 INTERNAL_ERROR（历史实现如此）。
                log.error(FAILED_TO_FETCH_ATTRIBUTES, e);
                sendError(sessionRef, cmd.getCmdId(), SubscriptionErrorCode.INTERNAL_ERROR, FAILED_TO_FETCH_ATTRIBUTES);
            }
        };


        // 鉴权通过后拉「该实体全部属性」（无 keys 过滤）。无 scope 查三个范围再合并。
        if (StringUtils.isEmpty(cmd.getScope())) {
            accessValidator.validate(sessionRef.getSecurityCtx(), Operation.READ_ATTRIBUTES, entityId, getAttributesFetchCallback(sessionRef.getSecurityCtx().getTenantId(), entityId, callback));
        } else {
            accessValidator.validate(sessionRef.getSecurityCtx(), Operation.READ_ATTRIBUTES, entityId, getAttributesFetchCallback(sessionRef.getSecurityCtx().getTenantId(), entityId, cmd.getScope(), callback));
        }
    }

    /**
     * 处理 v1 时间序列订阅命令。
     *
     * <p>先占配额；unsubscribe 则取消；否则按 keys 是否存在分流：
     * 指定键走窗口查询或最新值查询，空 keys 则订阅该实体全部最新遥测。
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

    /**
     * 按指定遥测键创建时序订阅。
     *
     * <p>{@code timeWindow > 0} 时先查 [startTs, startTs+timeWindow] 窗口内的历史点再订阅后续增量；
     * 否则只拉各键最新值。两种路径最终都通过 {@link #getSubscriptionCallback} 注册订阅并回推快照。
     */
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

    /**
     * 订阅实体全部最新遥测（命令未带 keys）。拉取 {@code findAllLatest} 后以 {@code allKeys=true} 注册订阅。
     */
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

    /**
     * 构造 v1 时间序列订阅对象。
     *
     * @param subState 各键上次已知时间戳，用于增量过滤
     * @param subLock  与快照发送共用的锁，防止增量抢在快照之前发出
     * @param allKeys  {@code true} 表示订阅后续出现的新键；{@code false} 仅跟踪已列出的键
     * @return 尚未注册到本地订阅服务的订阅实例
     */
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

    /**
     * 指定键时序查询成功后的回调：用查询结果填充 keyStates、注册订阅、回推快照。
     * 未查到的键以 {@code startTs} 作为初始时间戳，避免把更早的历史点当成新数据推送。
     * 租户读限流失败只打 trace，其它失败回 INTERNAL_ERROR。
     */
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

    /**
     * 取消一条 v1 订阅。
     *
     * <p>实体 id 为空视为会话级异常，直接清理整个会话。
     * 否则从 {@link #sessionCmdMap} 取出内部 subscriptionId（找不到则回退用 cmdId），
     * 再通知本地订阅服务取消。
     */
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

    /**
     * 按 sessionId 做会话级全量清理：移除元数据、取消全部 v1 订阅、删除 cmdId 映射、取消全部 v2 实体数据订阅。
     * 不负责配额表回收，配额由 {@link #processSessionClose} 在 CLOSED 事件中处理。
     */
    private void cleanupSessionById(TenantId tenantId, String sessionId) {
        wsSessionsMap.remove(sessionId);
        oldSubService.cancelAllSessionSubscriptions(tenantId, sessionId);
        sessionCmdMap.remove(sessionId);
        entityDataSubService.cancelAllSessionSubscriptions(sessionId);
    }

    /** v2 实体数据命令校验：query 为空且没有任何子命令时拒绝。 */
    private boolean validateSubscriptionCmd(WebSocketSessionRef sessionRef, EntityDataCmd cmd) {
        return validateCmd(sessionRef, cmd, () -> {
            if (cmd.getQuery() == null && !cmd.hasAnyCmd()) {
                throw new IllegalArgumentException("Query is empty!");
            }
        });
    }

    /** v2 实体计数命令校验：query 必填。 */
    private boolean validateSubscriptionCmd(WebSocketSessionRef sessionRef, EntityCountCmd cmd) {
        return validateCmd(sessionRef, cmd, () -> {
            if (cmd.getQuery() == null) {
                throw new IllegalArgumentException("Query is empty!");
            }
        });
    }

    /** v2 告警数据命令校验：query 必填。 */
    private boolean validateSubscriptionCmd(WebSocketSessionRef sessionRef, AlarmDataCmd cmd) {
        return validateCmd(sessionRef, cmd, () -> {
            if (cmd.getQuery() == null) {
                throw new IllegalArgumentException("Query is empty!");
            }
        });
    }

    /** v1 订阅命令校验：实体 id 不能为空。 */
    private boolean validateSubscriptionCmd(WebSocketSessionRef sessionRef, SubscriptionCmd cmd) {
        return validateCmd(sessionRef, cmd, () -> {
            if (cmd.getEntityId() == null || cmd.getEntityId().isEmpty()) {
                throw new IllegalArgumentException("Device id is empty!");
            }
        });
    }

    /**
     * 确认会话仍登记在 {@link #wsSessionsMap} 中。
     * 连接已关或 ESTABLISHED 尚未处理完时会失败，并向客户端回「Session meta-data not found」。
     *
     * @return {@code true} 表示会话有效，可以继续处理命令
     */
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

    /** 仅校验 cmdId 非负，不做业务字段检查。 */
    private boolean validateCmd(WebSocketSessionRef sessionRef, WsCmd cmd) {
        return validateCmd(sessionRef, cmd, null);
    }

    /**
     * 通用命令校验：cmdId 必须非负；若提供 {@code validator} 则执行额外业务检查。
     * 失败时向该 cmdId 回 BAD_REQUEST，并返回 {@code false}。
     */
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

    /**
     * 将更新对象序列化为 JSON，提交到 {@link #executor} 后由 {@link WebSocketMsgEndpoint} 写出。
     * 序列化失败只记日志；写出 IO 异常同样只记日志，不向上抛，避免拖垮订阅回调线程。
     */
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
     * Ping 定时任务：遍历当前所有会话，把发送提交到线程池。
     * 单条失败不影响其它会话。超时判定与断线清理由端点侧结合 {@link #cleanupIfStale} 完成。
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

    /**
     * 将命令中逗号分隔的 keys 字符串解析为集合。空字符串视为「未指定键」（订阅全部）。
     */
    private static Optional<Set<String>> getKeys(TelemetryPluginCmd cmd) {
        if (!StringUtils.isEmpty(cmd.getKeys())) {
            Set<String> keys = new HashSet<>();
            Collections.addAll(keys, cmd.getKeys().split(","));
            return Optional.of(keys);
        } else {
            return Optional.empty();
        }
    }

    /**
     * 合并多个属性查询 Future（例如同时查 CLIENT/SERVER/SHARED 三个 scope）为一份扁平列表。
     * 某个 scope 失败时 {@code successfulAsList} 对应位置为 null，会被跳过。
     */
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

    /**
     * 权限校验通过后：在全部 {@link AttributeScope} 中按指定 keys 查询，再合并结果交给业务回调。
     */
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

    /**
     * 权限校验通过后：仅在指定 scope 下按 keys 查询属性。
     */
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

    /**
     * 权限校验通过后：在全部 scope 拉取该实体全部属性（无 keys 过滤）。
     */
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

    /**
     * 权限校验通过后：仅在指定 scope 拉取该实体全部属性。
     */
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

    /**
     * 把 {@link AccessValidator} 的校验结果转成「成功则执行业务 / 失败则走 failure」的回调。
     * 校验结果不是 OK 时，通过 {@link ValidationCallback#getException} 还原异常再交给 failure。
     */
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


    /**
     * 解析客户端传入的聚合类型字符串。空值回退为 {@link Aggregation#NONE}。
     *
     * @param agg 聚合枚举名，例如 AVG、MAX；允许为 null 或空串
     */
    public static Aggregation getAggregation(String agg) {
        return StringUtils.isEmpty(agg) ? DEFAULT_AGGREGATION : Aggregation.valueOf(agg);
    }

    /**
     * 查询条数：客户端传 0 表示使用默认上限 {@link #DEFAULT_LIMIT}。
     */
    private int getLimit(int limit) {
        return limit == 0 ? DEFAULT_LIMIT : limit;
    }

    /**
     * 读取当前会话所属租户的默认画像配置，用于判断各层 WebSocket 订阅配额是否启用及上限值。
     * 租户或画像不存在时返回 null，调用方按「不限流」处理。
     */
    private DefaultTenantProfileConfiguration getTenantProfileConfiguration(WebSocketSessionRef sessionRef) {
        return Optional.ofNullable(tenantProfileCache.get(sessionRef.getSecurityCtx().getTenantId()))
                .map(TenantProfile::getDefaultProfileConfiguration).orElse(null);
    }

    /**
     * 将具体处理方法包装为类型安全的 {@link WsCmdHandler}，供 {@link #cmdsHandlers} 注册。
     */
    public static <C extends WsCmd> WsCmdHandler<C> newCmdHandler(BiConsumer<WebSocketSessionRef, C> handler) {
        return new WsCmdHandler<>(handler);
    }

    /**
     * 命令处理器适配器：把父类型 {@link WsCmd} 强制转换为注册时的具体命令类型后交给处理函数。
     * 调用方必须保证 {@link WsCmdType} 与命令运行时类型一致，否则会出现 {@link ClassCastException}。
     */
    @RequiredArgsConstructor
    @Getter
    @SuppressWarnings("unchecked")
    public static class WsCmdHandler<C extends WsCmd> {
        protected final BiConsumer<WebSocketSessionRef, C> handler;

        /**
         * 分发一条已路由到本处理器的命令。
         *
         * @param sessionRef 当前会话
         * @param cmd        原始命令，内部会转型为 {@code C}
         */
        public void handle(WebSocketSessionRef sessionRef, WsCmd cmd) {
            handler.accept(sessionRef, (C) cmd);
        }
    }

}
