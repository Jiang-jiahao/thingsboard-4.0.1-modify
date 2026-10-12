package com.jnks.iot.server.common.transport.service;

import lombok.Data;
import com.jnks.iot.server.common.transport.SessionMsgListener;
import com.jnks.iot.server.gen.transport.TransportProtos;

import java.util.concurrent.ScheduledFuture;

/**
 * Created by ashvayka on 15.10.18.
 */
@Data
public class SessionMetaData {

    private volatile TransportProtos.SessionInfoProto sessionInfo;
    private final TransportProtos.SessionType sessionType;
    private final SessionMsgListener listener;

    private volatile ScheduledFuture scheduledFuture;
    private volatile boolean subscribedToAttributes;
    private volatile boolean subscribedToRPC;
    private volatile boolean overwriteActivityTime;
    /**
     * 豁免"不活动过期"（{@code transport.sessions.inactivity_timeout}）。
     * <p>
     * 给"按设计不会报活动"的会话用 —— 典型是 UDP 给从不开口的被动设备建的虚拟下发会话：
     * 它不报活动是**有意**的（不伪造设备活跃），但通用过期会因此每 inactivity_timeout 把它收掉，
     * 周期性打断唯一的下发通道（线上实测每 10 分钟断 ~28 秒，落进去的 RPC 直接 NO_ACTIVE_CONNECTION）。
     * <p>
     * 实现见 {@link TransportActivityManager#updateState}：只把活动时间戳往前拨，不产生活动语义 ——
     * Core 的 {@code handleSessionActivity} 对没发过 OPEN 的会话整个是空操作（本就刻意不记活动）。
     */
    private volatile boolean expiryExempt;

    SessionMetaData(TransportProtos.SessionInfoProto sessionInfo, TransportProtos.SessionType sessionType, SessionMsgListener listener) {
        this.sessionInfo = sessionInfo;
        this.sessionType = sessionType;
        this.listener = listener;
        this.scheduledFuture = null;
    }

    void setScheduledFuture(ScheduledFuture scheduledFuture) {
        this.scheduledFuture = scheduledFuture;
    }

    public ScheduledFuture getScheduledFuture() {
        return scheduledFuture;
    }

    public boolean hasScheduledFuture() {
        return null != this.scheduledFuture;
    }
}
