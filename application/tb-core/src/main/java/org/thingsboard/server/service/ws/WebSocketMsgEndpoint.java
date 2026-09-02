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

import java.io.IOException;

/**
 * WebSocket 底层消息端点：真正往 TCP/WebSocket 连接写数据、发 Ping、关连接。
 *
 * <p>本接口与 {@link WebSocketService} 分层：
 * <ul>
 *   <li>{@link WebSocketService} 负责会话元数据、命令路由、订阅与业务更新编码；</li>
 *   <li>本接口只关心「这条消息如何落到具体连接上」，不解析命令、不维护订阅。</li>
 * </ul>
 *
 * <p>实现类是 {@link org.thingsboard.server.controller.plugin.TbWebSocketHandler}。
 * 业务层持有的是 {@link WebSocketSessionRef#getSessionId()}（外部会话 id），
 * 端点内部再映射到 Spring 的 {@code WebSocketSession#getId()} 后写出。
 *
 * <p>调用约定：
 * <ul>
 *   <li>{@code send} / {@code sendPing} / {@code close} 在会话找不到时通常只打日志，不抛业务异常；</li>
 *   <li>底层写出失败以 {@link IOException} 或关闭连接的方式体现；</li>
 *   <li>{@link #isOpen(String)} 供 {@link WebSocketService#cleanupIfStale} 判断连接是否已死、是否需要补偿清理。</li>
 * </ul>
 *
 * @see WebSocketService
 * @see org.thingsboard.server.controller.plugin.TbWebSocketHandler
 */
public interface WebSocketMsgEndpoint {

    /**
     * 向指定会话异步写出一条已编码的文本消息（通常是 JSON 订阅更新或错误）。
     *
     * <p>实现侧可能做每会话下行限流：超限时不再投递后续更新，并可能先发一条 TOO_MANY_UPDATES 错误。
     * 出站队列满时会关闭会话。
     *
     * @param sessionRef     目标会话（用外部 sessionId 定位连接）
     * @param subscriptionId 对应的客户端 cmdId / 订阅 id，限流错误回包时会带上，便于前端对号入座
     * @param msg            已序列化的文本载荷
     * @throws IOException 定位到连接后写出失败，或关闭连接时发生 IO 错误
     */
    void send(WebSocketSessionRef sessionRef, int subscriptionId, String msg) throws IOException;

    /**
     * 按当前时间对指定会话做一次保活探测。
     *
     * <p>实现通常根据距上次活动（含 Pong）的间隔决定：空闲未到阈值则忽略；
     * 达到探测间隔则发 WebSocket Ping；超过总超时则关闭会话。
     *
     * @param sessionRef  目标会话
     * @param currentTime 本次探测的时间戳（毫秒），由调用方传入以保持同一轮 Ping 任务时间一致
     * @throws IOException 关闭超时会话时发生 IO 错误
     */
    void sendPing(WebSocketSessionRef sessionRef, long currentTime) throws IOException;

    /**
     * 主动关闭指定会话的底层连接。
     *
     * <p>配额超限、认证失败、载荷无法解析、Ping 超时等场景都会走到这里。
     * {@code reason} 会作为 WebSocket Close 帧的状态码与原因字符串发给对端。
     *
     * @param sessionRef 目标会话
     * @param withReason 关闭状态（如 POLICY_VIOLATION、BAD_DATA、SESSION_NOT_RELIABLE）
     * @throws IOException 底层 close 调用失败
     */
    void close(WebSocketSessionRef sessionRef, CloseStatus withReason) throws IOException;

    /**
     * 判断外部 sessionId 对应的底层连接是否仍处于打开状态。
     *
     * <p>会话映射缺失或 Spring 会话已关时返回 {@code false}。
     * {@link WebSocketService} 用它识别「业务侧仍有记录、连接已断」的陈旧会话并做补偿清理。
     *
     * @param sessionId 外部会话 id（{@link WebSocketSessionRef#getSessionId()}）
     * @return {@code true} 表示连接仍打开
     */
    boolean isOpen(String sessionId);
}
