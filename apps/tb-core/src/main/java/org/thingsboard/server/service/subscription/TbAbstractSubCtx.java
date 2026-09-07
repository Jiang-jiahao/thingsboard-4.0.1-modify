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
package org.thingsboard.server.service.subscription;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.id.UserId;
import org.thingsboard.server.service.ws.WebSocketService;
import org.thingsboard.server.service.ws.WebSocketSessionRef;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.CmdUpdate;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * v2 查询型订阅上下文的最底层：一条前端 {@code cmdId} 对应一个 ctx。
 *
 * <p>放在 {@link DefaultTbEntityDataSubscriptionService#subscriptionsBySessionId} 里，
 * 只记住「这是哪条 WS、哪条命令」，以及怎么把 {@link CmdUpdate} 推回去。
 * 不持有实体列表，也不往订阅柜子里塞 {@code TbSubscription}——那是子类的事。
 *
 * <h2>继承关系（从上到下能力递增）</h2>
 * <pre>
 * TbAbstractSubCtx                          会话 + cmdId + 下行锁
 *   └── TbAbstractEntityQuerySubCtx         + EntityCountQuery、动态过滤值、刷新任务
 *         └── TbAbstractDataSubCtx          + 本页 EntityData、按实体拆内部订阅
 *               └── TbEntityDataSubCtx      实体表/时序图的具体增量合并
 * </pre>
 * 告警状态 {@code TbAlarmStatusSubCtx} 直接继承本类（没有 query 分页）。
 * 实体计数 {@code TbEntityCountSubCtx} 停在 {@code TbAbstractEntityQuerySubCtx}（只有数量，没有行）。
 *
 * <p>{@link #wsLock} 是公平锁，同一 ctx 上「查库回调推快照」和「柜子增量回调」互斥，避免乱序。
 *
 * @see DefaultTbEntityDataSubscriptionService
 * @see TbAbstractEntityQuerySubCtx
 */
@Slf4j
@Data
public abstract class TbAbstractSubCtx {

    /**
     * 本 ctx 下行互斥锁（公平）。{@link #sendWsMsg} 以及子类填完 EntityData 再推包时都要拿它。
     */
    @Getter
    protected final Lock wsLock = new ReentrantLock(true);

    /** 本 Core 节点 id，子类创建 {@code TbSubscription} 时写入，集群内识别归属。 */
    protected final String serviceId;

    /** 查询次数/耗时统计，动态刷新和 fetch 时累加。 */
    protected final SubscriptionServiceStatistics stats;

    /** 编码后的更新经此下发，不直接碰 WebSocketSession。 */
    private final WebSocketService wsService;

    /** 本地订阅柜子。子类 add/cancel 内部订阅都走这里。 */
    protected final TbLocalSubscriptionService localSubscriptionService;

    /** 这条 WS 的业务引用（外部 sessionId、登录用户）。 */
    protected final WebSocketSessionRef sessionRef;

    /** 前端这条命令的 cmdId。本类表按它索引 ctx；发给前端的更新也带这个。 */
    protected final int cmdId;

    /**
     * 已停止。取消订阅、会话关闭、动态刷新校验失败后置位。
     * 置位后不应再挂新的 refreshTask，也不应再处理增量。
     */
    protected volatile boolean stopped;

    /** ctx 创建时刻（毫秒）。内部订阅的 {@code queryTs} 用它过滤「查询开始前」的旧变更。 */
    @Getter
    protected long createdTime;

    public TbAbstractSubCtx(String serviceId, WebSocketService wsService,
                            TbLocalSubscriptionService localSubscriptionService,
                            SubscriptionServiceStatistics stats,
                            WebSocketSessionRef sessionRef, int cmdId) {
        this.createdTime = System.currentTimeMillis();
        this.serviceId = serviceId;
        this.wsService = wsService;
        this.localSubscriptionService = localSubscriptionService;
        this.stats = stats;
        this.sessionRef = sessionRef;
        this.cmdId = cmdId;
    }

    /**
     * 是否需要周期性重跑 query（动态页、告警时间窗等）。
     * 调度器据此统计当前动态 ctx 数量；具体是否挂任务由 Service 在 handleCmd 里决定。
     */
    public abstract boolean isDynamic();

    /**
     * 标记停止。子类会叠加：取消定时任务、从柜子清掉本 ctx 建的内部订阅。
     */
    public void stop() {
        stopped = true;
    }

    public String getSessionId() {
        return sessionRef.getSessionId();
    }

    public TenantId getTenantId() {
        return sessionRef.getSecurityCtx().getTenantId();
    }

    public CustomerId getCustomerId() {
        return sessionRef.getSecurityCtx().getCustomerId();
    }

    public UserId getUserId() {
        return sessionRef.getSecurityCtx().getId();
    }

    /**
     * 向本命令对应的前端推一条 v2 更新。持 {@link #wsLock}，与订阅回调推包互斥。
     */
    public void sendWsMsg(CmdUpdate update) {
        wsLock.lock();
        try {
            wsService.sendUpdate(sessionRef.getSessionId(), update);
        } finally {
            wsLock.unlock();
        }
    }

}
