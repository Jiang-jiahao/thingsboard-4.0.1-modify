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
