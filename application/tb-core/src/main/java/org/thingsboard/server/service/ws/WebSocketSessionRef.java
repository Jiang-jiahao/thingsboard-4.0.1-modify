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

import lombok.Builder;
import lombok.Data;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.service.security.model.SecurityUser;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 业务层对一条 WebSocket 连接的引用，不持有 Spring {@code WebSocketSession} 本身。
 *
 * <p>由 {@link org.thingsboard.server.controller.plugin.TbWebSocketHandler} 在握手成功时创建，
 * 之后在命令处理、订阅注册、下行推送、关闭整条链路上作为「这是哪条会话」的通行证传递。
 * 真正写 socket 时，Handler 再用 {@link #sessionId} 反查底层连接。
 *
 * <h2>和另外两份「会话元数据」的区别</h2>
 * <ul>
 *   <li>本类：身份（外部 sessionId）、登录用户、端点类型、内部订阅序号；</li>
 *   <li>{@link WsSessionMetaData}：{@link DefaultWebSocketService} 侧「这条会话还活着」的登记项；</li>
 *   <li>{@code TbWebSocketHandler.SessionMetaData}：连接层出站队列、异步发送、Ping 活动时间。</li>
 * </ul>
 *
 * <p>相等性只比较 {@link #sessionId}。{@link #securityCtx} 在握手未带 token 时为 null，
 * 首条 {@code AuthCmd} 校验通过后才会写入，因此字段是 {@code volatile}。
 *
 * @see WsSessionMetaData
 * @see WebSocketService
 * @see org.thingsboard.server.controller.plugin.TbWebSocketHandler
 */
@Builder
@Data
public class WebSocketSessionRef {

    private static final long serialVersionUID = 1L;

    /**
     * 外部会话 id（UUID）。业务层、订阅服务、下行 {@link WebSocketMsgEndpoint#send} 都用这个定位会话。
     * 与 Spring {@code WebSocketSession#getId()} 不是同一个值；Handler 内有 external → internal 映射。
     */
    private final String sessionId;

    /**
     * 当前登录用户。握手 URL 带合法 {@code token=} 时一开始就有值；
     * 否则等客户端首条命令里的 AuthCmd 成功后再 {@code setSecurityCtx}。
     * 未认证前业务层不应处理订阅命令。
     */
    private volatile SecurityUser securityCtx;

    /** 本节点接受连接时的本地地址，主要用于日志。 */
    private final InetSocketAddress localAddress;

    /** 客户端对端地址，主要用于日志。 */
    private final InetSocketAddress remoteAddress;

    /**
     * 由握手路径决定，影响入站 JSON 怎么反序列化：
     * {@code /api/ws} 为 {@link WebSocketSessionType#GENERAL}；
     * 旧路径 {@code /api/ws/plugins/telemetry|notifications} 对应已废弃的 TELEMETRY / NOTIFICATIONS。
     */
    private final WebSocketSessionType sessionType;

    /**
     * 本会话内部订阅 id 的递增序列。v1/v2 向 {@link org.thingsboard.server.service.subscription.TbLocalSubscriptionService}
     * 注册时都从这里取号，保证同一会话内 subscriptionId 唯一。
     * 前端只认 cmdId；cmdId → 本序号的映射记在 {@link DefaultWebSocketService} 的 sessionCmdMap 里。
     */
    private final AtomicInteger sessionSubIdSeq = new AtomicInteger();

    /**
     * 会话所属租户。尚未认证（{@code securityCtx == null}）时回退为系统租户 {@link TenantId#SYS_TENANT_ID}。
     */
    public TenantId getTenantId() {
        return securityCtx != null ? securityCtx.getTenantId() : TenantId.SYS_TENANT_ID;
    }

    /**
     * 仅按外部 {@link #sessionId} 判断是否同一条会话，忽略用户、地址等可变信息。
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WebSocketSessionRef that = (WebSocketSessionRef) o;
        return Objects.equals(sessionId, that.sessionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId);
    }

    /**
     * 日志用短格式：{@code [tenantId][userId][sessionId]}；未认证时只有 {@code [sessionId]}。
     */
    @Override
    public String toString() {
        String info = "";
        if (securityCtx != null) {
            info += "[" + securityCtx.getTenantId() + "]";
            info += "[" + securityCtx.getId() + "]";
        }
        info += "[" + sessionId + "]";
        return info;
    }

}
