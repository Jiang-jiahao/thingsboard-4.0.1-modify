package org.thingsboard.server.service.subscription;

import org.thingsboard.server.service.ws.WebSocketSessionRef;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.*;

/**
 * WebSocket 查询型实体数据订阅服务。
 * <p>
 * 处理 Dashboard 实体表、计数、告警数据/计数/状态等 WS 命令：执行查询、建立 latest/时序订阅并定时刷新动态页。
 *
 * @see DefaultTbEntityDataSubscriptionService
 */
public interface TbEntityDataSubscriptionService {

    /**
     * 处理实体数据订阅/查询命令。
     */
    void handleCmd(WebSocketSessionRef sessionId, EntityDataCmd cmd);

    /**
     * 处理实体计数订阅命令。
     */
    void handleCmd(WebSocketSessionRef sessionId, EntityCountCmd cmd);

    /**
     * 处理告警数据订阅命令。
     */
    void handleCmd(WebSocketSessionRef sessionId, AlarmDataCmd cmd);

    /**
     * 处理告警计数订阅命令。
     */
    void handleCmd(WebSocketSessionRef sessionId, AlarmCountCmd cmd);

    /**
     * 处理告警状态订阅命令。
     */
    void handleCmd(WebSocketSessionRef session, AlarmStatusCmd cmd);

    /**
     * 取消指定命令订阅。
     */
    void cancelSubscription(String sessionId, UnsubscribeCmd subscriptionId);

    /**
     * 取消会话全部查询型订阅。
     */
    void cancelAllSessionSubscriptions(String sessionId);

}
