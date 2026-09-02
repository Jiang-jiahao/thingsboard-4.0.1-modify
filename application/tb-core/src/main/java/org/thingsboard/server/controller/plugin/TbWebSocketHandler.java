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
package org.thingsboard.server.controller.plugin;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.websocket.RemoteEndpoint;
import jakarta.websocket.SendHandler;
import jakarta.websocket.SendResult;
import jakarta.websocket.Session;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.BeanCreationNotAllowedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.PongMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.adapter.NativeWebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.server.cache.limits.RateLimitService;
import org.thingsboard.server.common.data.TenantProfile;
import org.thingsboard.server.common.data.exception.ThingsboardErrorCode;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.id.UserId;
import org.thingsboard.server.common.data.limit.LimitedApi;
import org.thingsboard.server.common.data.tenant.profile.DefaultTenantProfileConfiguration;
import org.thingsboard.server.config.WebSocketConfiguration;
import org.thingsboard.server.dao.tenant.TbTenantProfileCache;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.security.auth.constants.WebSocketConstants;
import org.thingsboard.server.service.security.auth.jwt.JwtAuthenticationProvider;
import org.thingsboard.server.service.security.exception.JwtExpiredTokenException;
import org.thingsboard.server.service.security.model.SecurityUser;
import org.thingsboard.server.service.security.model.UserPrincipal;
import org.thingsboard.server.service.subscription.SubscriptionErrorCode;
import org.thingsboard.server.service.ws.AuthCmd;
import org.thingsboard.server.service.ws.SessionEvent;
import org.thingsboard.server.service.ws.WebSocketMsgEndpoint;
import org.thingsboard.server.service.ws.WebSocketService;
import org.thingsboard.server.service.ws.WebSocketSessionRef;
import org.thingsboard.server.service.ws.WebSocketSessionType;
import org.thingsboard.server.service.ws.WsCommandsWrapper;
import org.thingsboard.server.service.ws.notification.cmd.NotificationCmdsWrapper;
import org.thingsboard.server.service.ws.telemetry.cmd.TelemetryCmdsWrapper;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.thingsboard.server.service.ws.DefaultWebSocketService.NUMBER_OF_PING_ATTEMPTS;

/**
 * Spring WebSocket 处理器，同时作为 {@link WebSocketMsgEndpoint} 的唯一实现。
 *
 * <p>位于连接层与业务层之间：接收握手后的文本帧 / Ping-Pong / 开关连接事件，
 * 完成认证、会话配额、入站解析后转给 {@link WebSocketService}；
 * 业务层下行的 JSON、Ping、主动关闭再由本类落到具体 {@link WebSocketSession}。
 *
 * <h2>会话标识</h2>
 * 存在两套 id，必须成对维护：
 * <ul>
 *   <li><b>内部 id</b>：Spring {@code WebSocketSession#getId()}，连接层回调都带这个；</li>
 *   <li><b>外部 id</b>：本类生成的 UUID，写入 {@link WebSocketSessionRef}，业务层只认这个。</li>
 * </ul>
 * {@code internalSessionMap} 用内部 id 找 {@link SessionMetaData}；
 * {@code externalSessionMap} 用外部 id 反查内部 id，供 {@link #send} / {@link #sendPing} / {@link #close} 使用。
 *
 * <h2>认证时机</h2>
 * 握手 URL 可带 {@code ?token=}，通过则立刻建立业务会话；
 * 未带 token 时先进入 {@code pendingSessions}，必须在 {@code auth_timeout_ms} 内发来 {@link AuthCmd}，
 * 超时由 Caffeine 过期监听关闭连接。
 *
 * <h2>端点类型</h2>
 * 按握手路径区分 {@link WebSocketSessionType}，从而选择不同的命令 JSON 结构：
 * <ul>
 *   <li>{@code /api/ws} → GENERAL，直接反序列化为 {@link WsCommandsWrapper}；</li>
 *   <li>{@code /api/ws/plugins/telemetry} → TELEMETRY（已废弃路径）；</li>
 *   <li>{@code /api/ws/plugins/notifications} → NOTIFICATIONS（已废弃路径）。</li>
 * </ul>
 *
 * @see WebSocketService
 * @see WebSocketMsgEndpoint
 * @see WebSocketConfiguration
 */
@Service
@TbCoreComponent
@Slf4j
@RequiredArgsConstructor
public class TbWebSocketHandler extends TextWebSocketHandler implements WebSocketMsgEndpoint {

    /**
     * 已认证会话：Spring 内部 sessionId → 连接元数据（出站队列、异步写、活动时间）。
     * 文本帧、Pong、传输错误、关闭都先查这张表。
     */
    private final ConcurrentMap<String, SessionMetaData> internalSessionMap = new ConcurrentHashMap<>();

    /**
     * 外部 sessionId（业务层 UUID）→ Spring 内部 sessionId。
     * {@link WebSocketService} 下行时只带外部 id，本类靠此映射找到真正的连接。
     */
    private final ConcurrentMap<String, String> externalSessionMap = new ConcurrentHashMap<>();

    /**
     * 业务门面。使用 {@link Lazy} 打破与订阅/WebSocket 服务之间的循环依赖。
     */
    @Autowired @Lazy
    private WebSocketService webSocketService;

    /** 租户画像缓存，用于读取每租户/客户/用户的最大 WebSocket 会话数及每会话出站队列上限。 */
    @Autowired
    private TbTenantProfileCache tenantProfileCache;

    /** 每会话下行更新速率限制。超限会话会进入 {@link #blacklistedSessions}。 */
    @Autowired
    private RateLimitService rateLimitService;

    /** 用握手 query 或首条 {@link AuthCmd} 中的 JWT 换取 {@link SecurityUser}。 */
    @Autowired
    private JwtAuthenticationProvider authenticationProvider;

    /** 底层异步写出超时（毫秒）。超时未完成的 send 会被容器判定失败。 */
    @Value("${server.ws.send_timeout:5000}")
    private long sendTimeout;

    /**
     * Ping 总超时（毫秒）。与 {@link #NUMBER_OF_PING_ATTEMPTS} 配合：
     * 空闲超过 timeout/3 开始发 Ping，超过 timeout 仍无 Pong 则关闭。
     */
    @Value("${server.ws.ping_timeout:30000}")
    private long pingTimeout;

    /**
     * 每会话出站队列全局上限。租户画像里的 {@code wsMsgQueueLimitPerSession} 更小且大于 0 时，以画像为准。
     */
    @Value("${server.ws.max_queue_messages_per_session:1000}")
    private int wsMaxQueueMessagesPerSession;

    /** 未带 token 握手后，等待首条 AuthCmd 的最长时间（毫秒）。超时关闭连接。 */
    @Value("${server.ws.auth_timeout_ms:10000}")
    private int authTimeoutMs;

    /**
     * 下行更新被限流后的会话黑名单。首次超限会向客户端发 TOO_MANY_UPDATES，后续更新直接丢弃；
     * 限流窗口恢复后从本表移除，重新允许推送。
     */
    private final ConcurrentMap<String, WebSocketSessionRef> blacklistedSessions = new ConcurrentHashMap<>();

    /**
     * 租户级已建立会话集合（存 Spring 内部 sessionId）。
     * 仅当画像 {@code maxWsSessionsPerTenant > 0} 时启用。
     */
    private final ConcurrentMap<TenantId, Set<String>> tenantSessionsMap = new ConcurrentHashMap<>();

    /** 客户级已建立会话集合。仅客户用户且画像开启对应上限时使用。 */
    private final ConcurrentMap<CustomerId, Set<String>> customerSessionsMap = new ConcurrentHashMap<>();

    /** 普通登录用户（USER_NAME）的已建立会话集合。 */
    private final ConcurrentMap<UserId, Set<String>> regularUserSessionsMap = new ConcurrentHashMap<>();

    /** 公共用户（PUBLIC_ID，Dashboard 公开链接）的已建立会话集合。 */
    private final ConcurrentMap<UserId, Set<String>> publicUserSessionsMap = new ConcurrentHashMap<>();

    /**
     * 已握手但尚未认证的会话。写入后 {@code authTimeoutMs} 内未完成认证会被 Caffeine 过期并关闭。
     * 认证成功后必须 {@code invalidate}，否则过期监听仍可能误关已建立的会话。
     */
    private Cache<String, SessionMetaData> pendingSessions;

    /**
     * 构建待认证会话缓存：写入后固定 TTL，过期原因是 EXPIRED 时按 POLICY_VIOLATION 关闭连接。
     * 主动 invalidate（认证成功）不会走关闭逻辑。
     */
    @PostConstruct
    private void init() {
        pendingSessions = Caffeine.newBuilder()
                .expireAfterWrite(authTimeoutMs, TimeUnit.MILLISECONDS)
                .<String, SessionMetaData>removalListener((sessionId, sessionMd, removalCause) -> {
                    if (removalCause == RemovalCause.EXPIRED && sessionMd != null) {
                        try {
                            close(sessionMd.sessionRef, CloseStatus.POLICY_VIOLATION);
                        } catch (IOException e) {
                            log.warn("IO error", e);
                        }
                    }
                })
                .build();
    }

    /**
     * 节点停机时清空已认证会话映射。底层连接关闭由容器负责；此处避免停机后仍持有会话引用。
     */
    @PreDestroy
    private void stop() {
        internalSessionMap.clear();
    }

    /**
     * Spring 回调：收到一条文本帧。
     * 按内部 sessionId 找到元数据后入队处理；找不到说明会话已乱序关闭，直接 SERVER_ERROR 断开。
     */
    @Override
    public void handleTextMessage(WebSocketSession session, TextMessage message) {
        try {
            SessionMetaData sessionMd = getSessionMd(session.getId());
            if (sessionMd == null) {
                log.trace("[{}] Failed to find session", session.getId());
                session.close(CloseStatus.SERVER_ERROR.withReason("Session not found!"));
                return;
            }
            sessionMd.onMsg(message.getPayload());
        } catch (IOException e) {
            log.warn("IO error", e);
        }
    }

    /**
     * 解析并处理一条已出队的入站文本。
     *
     * <p>按会话类型选不同 Wrapper 反序列化；失败时：已认证则回 BAD_REQUEST 错误更新，未认证则关连接。
     * 已认证直接交给 {@link WebSocketService#handleCommands}；
     * 未认证必须带 {@link AuthCmd}，校验 JWT 成功后再 {@link #establishSession} 并处理同条消息里的其余命令。
     */
    void processMsg(SessionMetaData sessionMd, String msg) throws IOException {
        WebSocketSessionRef sessionRef = sessionMd.sessionRef;
        WsCommandsWrapper cmdsWrapper;
        try {
            switch (sessionRef.getSessionType()) {
                case GENERAL:
                    cmdsWrapper = JacksonUtil.fromString(msg, WsCommandsWrapper.class);
                    break;
                case TELEMETRY:
                    cmdsWrapper = JacksonUtil.fromString(msg, TelemetryCmdsWrapper.class).toCommonCmdsWrapper();
                    break;
                case NOTIFICATIONS:
                    cmdsWrapper = JacksonUtil.fromString(msg, NotificationCmdsWrapper.class).toCommonCmdsWrapper();
                    break;
                default:
                    return;
            }
        } catch (Exception e) {
            log.debug("{} Failed to decode subscription cmd: {}", sessionRef, e.getMessage(), e);
            if (sessionRef.getSecurityCtx() != null) {
                webSocketService.sendError(sessionRef, 1, SubscriptionErrorCode.BAD_REQUEST, "Failed to parse the payload");
            } else {
                close(sessionRef, CloseStatus.BAD_DATA.withReason(e.getMessage()));
            }
            return;
        }

        if (sessionRef.getSecurityCtx() != null) {
            log.trace("{} Processing {}", sessionRef, msg);
            webSocketService.handleCommands(sessionRef, cmdsWrapper);
        } else {
            AuthCmd authCmd = cmdsWrapper.getAuthCmd();
            if (authCmd == null) {
                close(sessionRef, CloseStatus.POLICY_VIOLATION.withReason("Auth cmd is missing"));
                return;
            }
            log.trace("{} Authenticating session", sessionRef);
            SecurityUser securityCtx;
            try {
                securityCtx = authenticationProvider.authenticate(authCmd.getToken());
            } catch (Exception e) {
                close(sessionRef, CloseStatus.BAD_DATA.withReason(e.getMessage()));
                return;
            }
            sessionRef.setSecurityCtx(securityCtx);
            pendingSessions.invalidate(sessionMd.session.getId());
            establishSession(sessionMd.session, sessionRef, sessionMd);

            webSocketService.handleCommands(sessionRef, cmdsWrapper);
        }
    }

    /**
     * 收到 WebSocket Pong：刷新该会话 {@code lastActivityTime}，避免下一次 Ping 探测误判超时。
     * 找不到会话则关闭连接。
     */
    @Override
    protected void handlePongMessage(WebSocketSession session, PongMessage message) throws Exception {
        try {
            SessionMetaData sessionMd = getSessionMd(session.getId());
            if (sessionMd != null) {
                log.trace("{} Processing pong response {}", sessionMd.sessionRef, message.getPayload());
                sessionMd.processPongMessage(System.currentTimeMillis());
            } else {
                log.trace("[{}] Failed to find session", session.getId());
                session.close(CloseStatus.SERVER_ERROR.withReason("Session not found!"));
            }
        } catch (IOException e) {
            log.warn("IO error", e);
        }
    }

    /**
     * 握手成功后的入口：设置原生异步写出超时，根据路径和 query token 构造 {@link WebSocketSessionRef}，再尝试建立会话。
     *
     * <p>未知路径、JWT 过期或其它认证失败会立即关闭，不会进入 pending 或已认证表。
     */
    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        super.afterConnectionEstablished(session);
        try {
            if (session instanceof NativeWebSocketSession) {
                Session nativeSession = ((NativeWebSocketSession) session).getNativeSession(Session.class);
                if (nativeSession != null) {
                    nativeSession.getAsyncRemote().setSendTimeout(sendTimeout);
                }
            }
            WebSocketSessionRef sessionRef = toRef(session);
            log.debug("[{}][{}] Session opened from address: {}", sessionRef.getSessionId(), session.getId(), session.getRemoteAddress());
            establishSession(session, sessionRef, null);
        } catch (InvalidParameterException e) {
            log.warn("[{}] Failed to start session", session.getId(), e);
            session.close(CloseStatus.BAD_DATA.withReason(e.getMessage()));
        } catch (JwtExpiredTokenException e) {
            log.trace("[{}] Failed to start session", session.getId(), e);
            session.close(CloseStatus.SERVER_ERROR.withReason(e.getMessage()));
        } catch (Exception e) {
            log.warn("[{}] Failed to start session", session.getId(), e);
            session.close(CloseStatus.SERVER_ERROR.withReason(e.getMessage()));
        }
    }

    /**
     * 将会话登记到内部映射，并视认证状态分流。
     *
     * <p>已有安全上下文：先做会话数配额，通过后写入 {@code internalSessionMap}/{@code externalSessionMap}，
     * 并向 {@link WebSocketService} 发 ESTABLISHED。失败则已在 {@link #checkLimits} 内关连接。
     * <p>尚未认证：只放入 {@code pendingSessions} 与外部映射，等待首条 AuthCmd；不通知业务层，避免未授权订阅。
     *
     * @param sessionMd 消息认证路径上已有的元数据；握手阶段传 {@code null}，由本方法新建
     */
    private void establishSession(WebSocketSession session, WebSocketSessionRef sessionRef, SessionMetaData sessionMd) throws IOException {
        if (sessionRef.getSecurityCtx() != null) {
            if (!checkLimits(session, sessionRef)) {
                return;
            }
            int maxMsgQueueSize = Optional.ofNullable(getTenantProfileConfiguration(sessionRef))
                    .map(DefaultTenantProfileConfiguration::getWsMsgQueueLimitPerSession)
                    .filter(profileLimit -> profileLimit > 0 && profileLimit < wsMaxQueueMessagesPerSession)
                    .orElse(wsMaxQueueMessagesPerSession);
            if (sessionMd == null) {
                sessionMd = new SessionMetaData(session, sessionRef);
            }
            sessionMd.setMaxMsgQueueSize(maxMsgQueueSize);

            internalSessionMap.put(session.getId(), sessionMd);
            externalSessionMap.put(sessionRef.getSessionId(), session.getId());
            processInWebSocketService(sessionRef, SessionEvent.onEstablished());
            log.info("[{}][{}][{}][{}] Session established from address: {}", sessionRef.getSecurityCtx().getTenantId(),
                    sessionRef.getSecurityCtx().getId(), sessionRef.getSessionId(), session.getId(), session.getRemoteAddress());
        } else {
            sessionMd = new SessionMetaData(session, sessionRef);
            pendingSessions.put(session.getId(), sessionMd);
            externalSessionMap.put(sessionRef.getSessionId(), session.getId());
        }
    }

    /**
     * 传输层错误（断连、协议错误等）。有元数据则转成 ERROR 会话事件给业务层，本方法不主动 close。
     */
    @Override
    public void handleTransportError(WebSocketSession session, Throwable tError) throws Exception {
        super.handleTransportError(session, tError);
        SessionMetaData sessionMd = getSessionMd(session.getId());
        if (sessionMd != null) {
            processInWebSocketService(sessionMd.sessionRef, SessionEvent.onError(tError));
        } else {
            log.trace("[{}] Failed to find session", session.getId());
        }
        log.trace("[{}] Session transport error", session.getId(), tError);
    }

    /**
     * 连接关闭：从已认证表或 pending 缓存移除，删除外部映射。
     * 仅已认证会话需要回收会话配额，并通知业务层 CLOSED 以取消订阅。
     */
    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus closeStatus) throws Exception {
        super.afterConnectionClosed(session, closeStatus);
        SessionMetaData sessionMd = internalSessionMap.remove(session.getId());
        if (sessionMd == null) {
            sessionMd = pendingSessions.asMap().remove(session.getId());
        }
        if (sessionMd != null) {
            externalSessionMap.remove(sessionMd.sessionRef.getSessionId());
            if (sessionMd.sessionRef.getSecurityCtx() != null) {
                cleanupLimits(session, sessionMd.sessionRef);
                processInWebSocketService(sessionMd.sessionRef, SessionEvent.onClosed());
            }
            log.info("{} Session is closed", sessionMd.sessionRef);
        } else {
            log.info("[{}] Session is closed", session.getId());
        }
    }

    /**
     * 把会话事件交给业务层。未认证会话不通知（业务层要求已有安全上下文）。
     * 停机过程中 Bean 可能已销毁，捕获 {@link BeanCreationNotAllowedException} 避免干扰关闭流程。
     */
    private void processInWebSocketService(WebSocketSessionRef sessionRef, SessionEvent event) {
        if (sessionRef.getSecurityCtx() == null) {
            return;
        }
        try {
            webSocketService.handleSessionEvent(sessionRef, event);
        } catch (BeanCreationNotAllowedException e) {
            log.warn("{} Failed to close session due to possible shutdown state", sessionRef);
        }
    }

    /**
     * 从握手 URI 构造会话引用：按路径判定 {@link WebSocketSessionType}，按 {@code token=} 尝试 JWT 认证。
     * 未知 plugin 路径抛 {@link InvalidParameterException}；token 非法由认证器抛错，均在建立连接处关闭。
     * 外部 sessionId 在此生成，之后对业务层不变。
     */
    private WebSocketSessionRef toRef(WebSocketSession session) {
        String path = session.getUri().getPath();
        WebSocketSessionType sessionType;
        if (path.equals(WebSocketConstants.WS_API_ENDPOINT)) {
            sessionType = WebSocketSessionType.GENERAL;
        } else {
            String type = StringUtils.substringAfter(path, WebSocketConstants.WS_PLUGINS_ENDPOINT);
            sessionType = WebSocketSessionType.forName(type)
                    .orElseThrow(() -> new InvalidParameterException("Unknown session type"));
        }

        SecurityUser securityCtx = null;
        String token = StringUtils.substringAfter(session.getUri().getQuery(), "token=");
        if (StringUtils.isNotEmpty(token)) {
            securityCtx = authenticationProvider.authenticate(token);
        }
        return WebSocketSessionRef.builder()
                .sessionId(UUID.randomUUID().toString())
                .securityCtx(securityCtx)
                .localAddress(session.getLocalAddress())
                .remoteAddress(session.getRemoteAddress())
                .sessionType(sessionType)
                .build();
    }

    /**
     * 按 Spring 内部 sessionId 取元数据：先查已认证表，再查 pending。
     * 文本帧、Pong、close/isOpen 都走这里，保证未认证会话也能收 AuthCmd、也能被主动关闭。
     */
    private SessionMetaData getSessionMd(String internalSessionId) {
        SessionMetaData sessionMd = internalSessionMap.get(internalSessionId);
        if (sessionMd == null) {
            sessionMd = pendingSessions.getIfPresent(internalSessionId);
        }
        return sessionMd;
    }

    /**
     * 单条 WebSocket 连接的运行时状态：入站串行处理、出站单飞异步写、Ping 活动时间。
     *
     * <p>实现 {@link SendHandler}，文本消息 {@code sendText} 完成后在 {@link #onResult} 里释放 {@code isSending}，
     * 再取下一条。Ping 是阻塞 {@code sendPing}，发完立刻释放并继续队列。
     * 任意时刻最多只有一条消息在飞，避免乱序和压垮对端。
     */
    class SessionMetaData implements SendHandler {
        /** Spring 会话，close / isOpen 用。 */
        private final WebSocketSession session;
        /** JSR-356 异步远端，实际 sendText / sendPing 走这里。 */
        private final RemoteEndpoint.Async asyncRemote;
        /** 暴露给业务层的会话引用。 */
        private final WebSocketSessionRef sessionRef;

        /**
         * 出站是否正在发送。{@code compareAndSet(false, true)} 保证单飞；
         * 文本发送完成或 Ping 发完后置回 false。
         */
        final AtomicBoolean isSending = new AtomicBoolean(false);
        /** 待写出的文本更新与 Ping。 */
        private final Queue<TbWebSocketMsg<?>> outboundMsgQueue = new ConcurrentLinkedQueue<>();
        /** 出站队列长度，与 {@link #maxMsgQueueSize} 比较决定是否拒收并关连接。 */
        private final AtomicInteger outboundMsgQueueSize = new AtomicInteger();
        /** 本会话出站队列上限，建立会话时按画像与全局配置取较小者。 */
        @Setter
        private int maxMsgQueueSize = wsMaxQueueMessagesPerSession;

        /**
         * 入站文本队列。WebSocket 回调线程只负责入队，真正解析在持有 {@link #inboundMsgQueueProcessorLock} 的线程串行执行，
         * 避免同一会话并发 {@code processMsg}（尤其是认证与后续命令交叉）。
         */
        private final Queue<String> inboundMsgQueue = new ConcurrentLinkedQueue<>();
        private final Lock inboundMsgQueueProcessorLock = new ReentrantLock();

        /** 最近一次活动时间：构造时、收到 Pong 时更新。Ping 探测用当前时间减此值判断是否超时。 */
        private volatile long lastActivityTime;

        SessionMetaData(WebSocketSession session, WebSocketSessionRef sessionRef) {
            super();
            this.session = session;
            Session nativeSession = ((NativeWebSocketSession) session).getNativeSession(Session.class);
            this.asyncRemote = nativeSession.getAsyncRemote();
            this.sessionRef = sessionRef;
            this.lastActivityTime = System.currentTimeMillis();
        }

        /**
         * 由业务层定时 Ping 任务触发。
         * 空闲 ≥ pingTimeout：关连接；空闲 ≥ pingTimeout/尝试次数：入队一条 Ping 帧；否则本轮不做任何事。
         */
        void sendPing(long currentTime) {
            try {
                long timeSinceLastActivity = currentTime - lastActivityTime;
                if (timeSinceLastActivity >= pingTimeout) {
                    log.warn("{} Closing session due to ping timeout", sessionRef);
                    closeSession(CloseStatus.SESSION_NOT_RELIABLE);
                } else if (timeSinceLastActivity >= pingTimeout / NUMBER_OF_PING_ATTEMPTS) {
                    sendMsg(TbWebSocketPingMsg.INSTANCE);
                }
            } catch (Exception e) {
                log.trace("{} Failed to send ping msg", sessionRef, e);
                closeSession(CloseStatus.SESSION_NOT_RELIABLE);
            }
        }

        /**
         * 关闭本连接并清空尚未发出的出站队列，避免关闭后回调仍尝试发送。
         */
        void closeSession(CloseStatus reason) {
            try {
                close(this.sessionRef, reason);
            } catch (IOException ioe) {
                log.trace("{} Session transport error", sessionRef, ioe);
            } finally {
                outboundMsgQueue.clear();
            }
        }

        /** 客户端 Pong 到达，视为连接仍存活。 */
        void processPongMessage(long currentTime) {
            lastActivityTime = currentTime;
        }

        void sendMsg(String msg) {
            sendMsg(new TbWebSocketTextMsg(msg));
        }

        /**
         * 将消息放入出站队列。未超上限则尝试触发发送；已满则 POLICY_VIOLATION 关闭，防止内存被更新堆积打满。
         */
        void sendMsg(TbWebSocketMsg<?> msg) {
            if (outboundMsgQueueSize.get() < maxMsgQueueSize) {
                outboundMsgQueue.add(msg);
                outboundMsgQueueSize.incrementAndGet();
                processNextMsg();
            } else {
                log.info("{} Session closed due to updates queue size exceeded", sessionRef);
                closeSession(CloseStatus.POLICY_VIOLATION.withReason("Max pending updates limit reached!"));
            }
        }

        /**
         * 真正写出队首消息。文本走异步 sendText（完成回调 {@link #onResult}）；
         * Ping 走阻塞 sendPing，返回后立刻释放 isSending 并继续下一条。
         */
        private void sendMsgInternal(TbWebSocketMsg<?> msg) {
            try {
                if (TbWebSocketMsgType.TEXT.equals(msg.getType())) {
                    TbWebSocketTextMsg textMsg = (TbWebSocketTextMsg) msg;
                    this.asyncRemote.sendText(textMsg.getMsg(), this);
                } else {
                    TbWebSocketPingMsg pingMsg = (TbWebSocketPingMsg) msg;
                    this.asyncRemote.sendPing(pingMsg.getMsg());
                    isSending.set(false);
                    processNextMsg();
                }
            } catch (Exception e) {
                log.trace("{} Failed to send msg", sessionRef, e);
                closeSession(CloseStatus.SESSION_NOT_RELIABLE);
            }
        }

        /**
         * 异步文本发送完成。失败则关连接；成功则释放单飞标志并发送下一条。
         */
        @Override
        public void onResult(SendResult result) {
            if (!result.isOK()) {
                log.trace("{} Failed to send msg", sessionRef, result.getException());
                closeSession(CloseStatus.SESSION_NOT_RELIABLE);
                return;
            }

            isSending.set(false);
            processNextMsg();
        }

        /**
         * 若队列非空且当前没有飞行中的发送，则 CAS 抢到发送权后 poll 一条写出。
         * poll 到 null（并发清空）时把 isSending 还回去，避免卡死后续发送。
         */
        private void processNextMsg() {
            if (outboundMsgQueue.isEmpty() || !isSending.compareAndSet(false, true)) {
                return;
            }
            TbWebSocketMsg<?> msg = outboundMsgQueue.poll();
            if (msg != null) {
                outboundMsgQueueSize.decrementAndGet();
                sendMsgInternal(msg);
            } else {
                isSending.set(false);
            }
        }

        /**
         * 入站入口：只入队，再尝试抢锁串行 {@link TbWebSocketHandler#processMsg}。
         */
        public void onMsg(String msg) throws IOException {
            inboundMsgQueue.add(msg);
            tryProcessInboundMsgs();
        }

        /**
         * 用 tryLock 保证同一会话同时只有一个线程在解析入站消息。
         * 抢不到锁直接返回：持锁线程会把队列抽干。抽干后释放锁，避免认证与业务命令交错执行。
         */
        void tryProcessInboundMsgs() throws IOException {
            while (!inboundMsgQueue.isEmpty()) {
                if (inboundMsgQueueProcessorLock.tryLock()) {
                    try {
                        String msg;
                        while ((msg = inboundMsgQueue.poll()) != null) {
                            processMsg(this, msg);
                        }
                    } finally {
                        inboundMsgQueueProcessorLock.unlock();
                    }
                } else {
                    return;
                }
            }
        }
    }

    /**
     * 向指定会话写出已编码的文本更新。
     *
     * <p>外部 id → 内部 id → SessionMetaData。先做每会话下行 QPS 限流：
     * 超限且首次进入黑名单时发 TOO_MANY_UPDATES 错误 JSON，后续更新直接丢弃；
     * 限流恢复后移出黑名单并正常投递。找不到映射只打 warn。
     */
    @Override
    public void send(WebSocketSessionRef sessionRef, int subscriptionId, String msg) throws IOException {
        log.debug("{} Sending {}", sessionRef, msg);
        String externalId = sessionRef.getSessionId();
        String internalId = externalSessionMap.get(externalId);
        if (internalId != null) {
            SessionMetaData sessionMd = internalSessionMap.get(internalId);
            if (sessionMd != null) {
                TenantId tenantId = sessionRef.getSecurityCtx().getTenantId();
                if (!rateLimitService.checkRateLimit(LimitedApi.WS_UPDATES_PER_SESSION, tenantId, (Object) sessionRef.getSessionId())) {
                    if (blacklistedSessions.putIfAbsent(externalId, sessionRef) == null) {
                        log.info("{} Failed to process session update. Max session updates limit reached", sessionRef);
                        sessionMd.sendMsg("{\"subscriptionId\":" + subscriptionId + ", \"errorCode\":" + ThingsboardErrorCode.TOO_MANY_UPDATES.getErrorCode() + ", \"errorMsg\":\"Too many updates!\"}");
                    }
                    return;
                } else {
                    log.debug("{} Session is no longer blacklisted.", sessionRef);
                    blacklistedSessions.remove(externalId);
                }
                sessionMd.sendMsg(msg);
            } else {
                log.warn("[{}][{}] Failed to find session by internal id", externalId, internalId);
            }
        } else {
            log.warn("[{}] Failed to find session by external id", externalId);
        }
    }

    /**
     * 将保活探测转到对应 {@link SessionMetaData#sendPing(long)}。映射缺失只记日志，不抛错。
     */
    @Override
    public void sendPing(WebSocketSessionRef sessionRef, long currentTime) throws IOException {
        String externalId = sessionRef.getSessionId();
        String internalId = externalSessionMap.get(externalId);
        if (internalId != null) {
            SessionMetaData sessionMd = internalSessionMap.get(internalId);
            if (sessionMd != null) {
                sessionMd.sendPing(currentTime);
            } else {
                log.warn("[{}][{}] Failed to find session by internal id", externalId, internalId);
            }
        } else {
            log.warn("[{}] Failed to find session by external id", externalId);
        }
    }

    /**
     * 按外部 id 关闭底层连接。待认证会话也在 {@link #getSessionMd} 覆盖范围内，认证超时关闭走同一路径。
     */
    @Override
    public void close(WebSocketSessionRef sessionRef, CloseStatus reason) throws IOException {
        String externalId = sessionRef.getSessionId();
        log.debug("{} Processing close request", sessionRef.toString());
        String internalId = externalSessionMap.get(externalId);
        if (internalId != null) {
            SessionMetaData sessionMd = getSessionMd(internalId);
            if (sessionMd != null) {
                sessionMd.session.close(reason);
            } else {
                log.warn("[{}][{}] Failed to find session by internal id", externalId, internalId);
            }
        } else {
            log.warn("[{}] Failed to find session by external id", externalId);
        }
    }

    /**
     * 外部 sessionId 对应的 Spring 会话是否仍打开。映射或元数据缺失视为已关闭，供业务层补偿清理。
     */
    @Override
    public boolean isOpen(String externalId) {
        String internalId = externalSessionMap.get(externalId);
        if (internalId != null) {
            SessionMetaData sessionMd = getSessionMd(internalId);
            if (sessionMd != null) {
                return sessionMd.session.isOpen();
            }
        }
        return false;
    }

    /**
     * 已认证会话建立前的会话数配额检查。
     *
     * <p>按租户画像依次检查：租户总数 →（客户用户时）客户数、普通用户数、公共用户数。
     * 上限为 0 表示该层不限制。任一已启用层级已满则以 POLICY_VIOLATION 关闭并返回 {@code false}。
     * 通过的层级会把当前内部 sessionId 加入对应集合，关闭时由 {@link #cleanupLimits} 移除。
     *
     * @return {@code true} 允许建立业务会话
     */
    private boolean checkLimits(WebSocketSession session, WebSocketSessionRef sessionRef) throws IOException {
        var tenantProfileConfiguration = getTenantProfileConfiguration(sessionRef);
        if (tenantProfileConfiguration == null) {
            return true;
        }
        boolean limitAllowed;
        String sessionId = session.getId();
        if (tenantProfileConfiguration.getMaxWsSessionsPerTenant() > 0) {
            Set<String> tenantSessions = tenantSessionsMap.computeIfAbsent(sessionRef.getSecurityCtx().getTenantId(), id -> ConcurrentHashMap.newKeySet());
            synchronized (tenantSessions) {
                limitAllowed = tenantSessions.size() < tenantProfileConfiguration.getMaxWsSessionsPerTenant();
                if (limitAllowed) {
                    tenantSessions.add(sessionId);
                }
            }
            if (!limitAllowed) {
                log.info("{} Failed to start session. Max tenant sessions limit reached", sessionRef.toString());
                session.close(CloseStatus.POLICY_VIOLATION.withReason("Max tenant sessions limit reached!"));
                return false;
            }
        }

        if (sessionRef.getSecurityCtx().isCustomerUser()) {
            if (tenantProfileConfiguration.getMaxWsSessionsPerCustomer() > 0) {
                Set<String> customerSessions = customerSessionsMap.computeIfAbsent(sessionRef.getSecurityCtx().getCustomerId(), id -> ConcurrentHashMap.newKeySet());
                synchronized (customerSessions) {
                    limitAllowed = customerSessions.size() < tenantProfileConfiguration.getMaxWsSessionsPerCustomer();
                    if (limitAllowed) {
                        customerSessions.add(sessionId);
                    }
                }
                if (!limitAllowed) {
                    log.info("{} Failed to start session. Max customer sessions limit reached", sessionRef.toString());
                    session.close(CloseStatus.POLICY_VIOLATION.withReason("Max customer sessions limit reached"));
                    return false;
                }
            }
            if (tenantProfileConfiguration.getMaxWsSessionsPerRegularUser() > 0
                    && UserPrincipal.Type.USER_NAME.equals(sessionRef.getSecurityCtx().getUserPrincipal().getType())) {
                Set<String> regularUserSessions = regularUserSessionsMap.computeIfAbsent(sessionRef.getSecurityCtx().getId(), id -> ConcurrentHashMap.newKeySet());
                synchronized (regularUserSessions) {
                    limitAllowed = regularUserSessions.size() < tenantProfileConfiguration.getMaxWsSessionsPerRegularUser();
                    if (limitAllowed) {
                        regularUserSessions.add(sessionId);
                    }
                }
                if (!limitAllowed) {
                    log.info("{} Failed to start session. Max regular user sessions limit reached", sessionRef.toString());
                    session.close(CloseStatus.POLICY_VIOLATION.withReason("Max regular user sessions limit reached"));
                    return false;
                }
            }
            if (tenantProfileConfiguration.getMaxWsSessionsPerPublicUser() > 0
                    && UserPrincipal.Type.PUBLIC_ID.equals(sessionRef.getSecurityCtx().getUserPrincipal().getType())) {
                Set<String> publicUserSessions = publicUserSessionsMap.computeIfAbsent(sessionRef.getSecurityCtx().getId(), id -> ConcurrentHashMap.newKeySet());
                synchronized (publicUserSessions) {
                    limitAllowed = publicUserSessions.size() < tenantProfileConfiguration.getMaxWsSessionsPerPublicUser();
                    if (limitAllowed) {
                        publicUserSessions.add(sessionId);
                    }
                }
                if (!limitAllowed) {
                    log.info("{} Failed to start session. Max public user sessions limit reached", sessionRef.toString());
                    session.close(CloseStatus.POLICY_VIOLATION.withReason("Max public user sessions limit reached"));
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * 已认证会话关闭时的配额与限流回收：清每会话更新限流桶、移出黑名单，
     * 并从租户/客户/用户会话集合中删除当前内部 sessionId。
     */
    private void cleanupLimits(WebSocketSession session, WebSocketSessionRef sessionRef) {
        var tenantProfileConfiguration = getTenantProfileConfiguration(sessionRef);
        if (tenantProfileConfiguration == null) return;

        String sessionId = session.getId();
        rateLimitService.cleanUp(LimitedApi.WS_UPDATES_PER_SESSION, sessionRef.getSessionId());
        blacklistedSessions.remove(sessionRef.getSessionId());
        if (tenantProfileConfiguration.getMaxWsSessionsPerTenant() > 0) {
            Set<String> tenantSessions = tenantSessionsMap.computeIfAbsent(sessionRef.getSecurityCtx().getTenantId(), id -> ConcurrentHashMap.newKeySet());
            synchronized (tenantSessions) {
                tenantSessions.remove(sessionId);
            }
        }
        if (sessionRef.getSecurityCtx().isCustomerUser()) {
            if (tenantProfileConfiguration.getMaxWsSessionsPerCustomer() > 0) {
                Set<String> customerSessions = customerSessionsMap.computeIfAbsent(sessionRef.getSecurityCtx().getCustomerId(), id -> ConcurrentHashMap.newKeySet());
                synchronized (customerSessions) {
                    customerSessions.remove(sessionId);
                }
            }
            if (tenantProfileConfiguration.getMaxWsSessionsPerRegularUser() > 0 && UserPrincipal.Type.USER_NAME.equals(sessionRef.getSecurityCtx().getUserPrincipal().getType())) {
                Set<String> regularUserSessions = regularUserSessionsMap.computeIfAbsent(sessionRef.getSecurityCtx().getId(), id -> ConcurrentHashMap.newKeySet());
                synchronized (regularUserSessions) {
                    regularUserSessions.remove(sessionId);
                }
            }
            if (tenantProfileConfiguration.getMaxWsSessionsPerPublicUser() > 0 && UserPrincipal.Type.PUBLIC_ID.equals(sessionRef.getSecurityCtx().getUserPrincipal().getType())) {
                Set<String> publicUserSessions = publicUserSessionsMap.computeIfAbsent(sessionRef.getSecurityCtx().getId(), id -> ConcurrentHashMap.newKeySet());
                synchronized (publicUserSessions) {
                    publicUserSessions.remove(sessionId);
                }
            }
        }
    }

    /**
     * 读取会话所属租户的默认画像配置。租户或画像不存在时返回 null，配额检查按「不限制」处理。
     */
    private DefaultTenantProfileConfiguration getTenantProfileConfiguration(WebSocketSessionRef sessionRef) {
        return Optional.ofNullable(tenantProfileCache.get(sessionRef.getSecurityCtx().getTenantId()))
                .map(TenantProfile::getDefaultProfileConfiguration).orElse(null);
    }

}
