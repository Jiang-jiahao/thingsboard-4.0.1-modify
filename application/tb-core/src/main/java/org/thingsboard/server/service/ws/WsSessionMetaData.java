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


/**
 * {@link DefaultWebSocketService} 在本节点登记的一条已建立业务会话。
 *
 * <p>放在 {@code wsSessionsMap}（key 为外部 {@link WebSocketSessionRef#getSessionId()}）里，用来回答两件事：
 * <ul>
 *   <li>这条会话是否还被业务层认为有效（命令处理前会查这张表，找不到就回 Session meta-data not found）；</li>
 *   <li>下行 {@code sendUpdate}/{@code close} 时，如何拿回 {@link WebSocketSessionRef} 交给 {@link WebSocketMsgEndpoint}。</li>
 * </ul>
 *
 * <p>连接层还有另一份 {@code TbWebSocketHandler.SessionMetaData}（出站队列、Ping、异步写）。
 * 本类不碰 socket，只给业务门面做「会话还在不在」的索引。
 *
 * <p>ESTABLISHED 时创建并放入 map；CLOSED 或 {@link WebSocketService#cleanupIfStale} 时移除。
 * 未认证的 pending 连接不会进入这张表，因此 AuthCmd 完成前业务层收不到命令。
 *
 * @see WebSocketSessionRef
 * @see DefaultWebSocketService
 */
public class WsSessionMetaData {

    /** 本会话的业务引用，下行发送、关闭连接都通过它定位 Handler 侧的真实连接。 */
    private WebSocketSessionRef sessionRef;

    /**
     * 登记到业务层时的时间戳（毫秒）。
     * 当前 Ping 保活看的是 Handler 里那份 SessionMetaData 的活动时间，本字段创建后不再被读写。
     */
    private long lastActivityTime;

    /**
     * 会话建立时登记。{@code lastActivityTime} 取当前毫秒时间。
     *
     * @param sessionRef 已带安全上下文的会话引用（Handler 只在认证成功后才发 ESTABLISHED）
     */
    public WsSessionMetaData(WebSocketSessionRef sessionRef) {
        super();
        this.sessionRef = sessionRef;
        this.lastActivityTime = System.currentTimeMillis();
    }

    public WebSocketSessionRef getSessionRef() {
        return sessionRef;
    }

    public void setSessionRef(WebSocketSessionRef sessionRef) {
        this.sessionRef = sessionRef;
    }

    public long getLastActivityTime() {
        return lastActivityTime;
    }

    public void setLastActivityTime(long lastActivityTime) {
        this.lastActivityTime = lastActivityTime;
    }

    @Override
    public String toString() {
        return "WsSessionMetaData [sessionRef=" + sessionRef + ", lastActivityTime=" + lastActivityTime + "]";
    }
}
