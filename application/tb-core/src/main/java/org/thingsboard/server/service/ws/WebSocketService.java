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

import org.springframework.web.socket.CloseStatus;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.service.subscription.SubscriptionErrorCode;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.CmdUpdate;
import org.thingsboard.server.service.ws.telemetry.sub.TelemetrySubscriptionUpdate;

/**
 * Core 面向 UI 的 WebSocket 业务门面：会话生命周期、命令路由、订阅更新下发。
 *
 * <p>本接口不直接操作底层连接。连接层事件与文本帧由
 * {@link org.thingsboard.server.controller.plugin.TbWebSocketHandler} 接收后转入本服务；
 * 本服务编码后的下行消息再经 {@link WebSocketMsgEndpoint} 写出。
 *
 * <h2>调用方向</h2>
 * <ul>
 *   <li><b>上行</b>：Handler 在连接建立/出错/关闭时调用 {@link #handleSessionEvent}；
 *       解析出命令后调用 {@link #handleCommands}。</li>
 *   <li><b>下行</b>：订阅服务在属性/遥测/实体数据变化时调用 {@link #sendUpdate}；
 *       参数非法、权限失败、内部异常时调用 {@link #sendError}。</li>
 *   <li><b>控制</b>：配额超限等场景调用 {@link #close}；怀疑连接已死时调用 {@link #cleanupIfStale}。</li>
 * </ul>
 *
 * <p>默认实现为 {@link DefaultWebSocketService}。
 *
 * @see DefaultWebSocketService
 * @see WebSocketMsgEndpoint
 * @see org.thingsboard.server.controller.plugin.TbWebSocketHandler
 */
public interface WebSocketService {

    /**
     * 处理底层连接层上报的会话事件。
     *
     * <ul>
     *   <li>建立：登记会话元数据，此后才允许处理命令；</li>
     *   <li>错误：记录传输异常，不断开（断开由关闭事件处理）；</li>
     *   <li>关闭：取消该会话全部订阅，并回收订阅配额计数。</li>
     * </ul>
     *
     * @param sessionRef   会话引用（外部 sessionId、安全上下文、会话类型）
     * @param sessionEvent 建立 / 错误 / 关闭
     */
    void handleSessionEvent(WebSocketSessionRef sessionRef, SessionEvent sessionEvent);

    /**
     * 处理客户端一次上行消息中的命令列表。
     *
     * <p>空包装或空列表应直接返回。实现按 {@link WsCmdType} 分发到遥测、实体数据或通知处理器。
     * 单条命令失败不应中断同一次消息里的后续命令。
     *
     * @param sessionRef      当前会话
     * @param commandsWrapper 已反序列化的命令包装，内含多条 {@link WsCmd}
     */
    void handleCommands(WebSocketSessionRef sessionRef, WsCommandsWrapper commandsWrapper);

    /**
     * 向指定会话推送 v1 遥测/属性订阅更新。
     *
     * <p>本地订阅服务内部使用 subscriptionId，前端只认识 cmdId。
     * 实现发送前会把更新里的 subscriptionId 替换为 {@code cmdId}。
     *
     * @param sessionId 外部会话 id
     * @param cmdId     客户端命令 id
     * @param update    键值变更或错误信息
     */
    void sendUpdate(String sessionId, int cmdId, TelemetrySubscriptionUpdate update);

    /**
     * 向指定会话推送 v2 命令更新（实体数据增量、告警刷新、计数变化等）。
     * cmdId 取自 {@link CmdUpdate} 自身。
     *
     * @param sessionId 外部会话 id
     * @param update    v2 更新载荷
     */
    void sendUpdate(String sessionId, CmdUpdate update);

    /**
     * 向当前会话发送一条订阅错误更新。
     *
     * <p>用于权限失败、参数非法、会话元数据缺失、内部异常等。
     * 前端按 {@code subId}（通常等于 cmdId）匹配对应订阅。
     *
     * @param sessionRef 当前会话
     * @param subId      对应的 cmdId / 订阅 id
     * @param errorCode  协议层错误码
     * @param errorMsg   可读错误信息
     */
    void sendError(WebSocketSessionRef sessionRef, int subId, SubscriptionErrorCode errorCode, String errorMsg);

    /**
     * 主动关闭指定会话的底层连接。
     *
     * <p>实现通过 {@link WebSocketMsgEndpoint#close} 写出 Close 帧。
     * 典型场景：租户/客户/用户订阅数超限。
     *
     * @param sessionId 外部会话 id
     * @param status    关闭状态及原因
     */
    void close(String sessionId, CloseStatus status);

    /**
     * 底层连接已断开、但本服务仍保留会话记录时的补偿清理。
     *
     * <p>典型场景：对端异常掉线，CLOSED 事件未及时到达。
     * 实现应先用 {@link WebSocketMsgEndpoint#isOpen(String)} 确认连接已关，再取消订阅并移除元数据。
     *
     * @param tenantId  会话所属租户
     * @param sessionId 外部会话 id
     */
    void cleanupIfStale(TenantId tenantId, String sessionId);

}
