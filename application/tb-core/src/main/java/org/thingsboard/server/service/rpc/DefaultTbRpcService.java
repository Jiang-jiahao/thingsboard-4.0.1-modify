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
package org.thingsboard.server.service.rpc;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.server.cluster.TbClusterService;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.RpcId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.msg.TbMsgType;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.rpc.Rpc;
import org.thingsboard.server.common.data.rpc.RpcStatus;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.TbMsgMetaData;
import org.thingsboard.server.dao.rpc.RpcService;
import org.thingsboard.server.queue.util.TbCoreComponent;

/**
 * 持久化 RPC 实体服务。
 * <p>
 * 保存或更新 RPC 状态后，向规则引擎推送 {@code RPC_{STATUS}} 消息，供规则链感知调用进度。
 *
 * @see TbRpcService
 */
@TbCoreComponent
@Service
@RequiredArgsConstructor
@Slf4j
public class DefaultTbRpcService implements TbRpcService {
    private final RpcService rpcService;
    private final TbClusterService tbClusterService;

    /**
     * 保存 RPC 记录并推送状态消息到规则引擎。
     */
    @Override
    public Rpc save(TenantId tenantId, Rpc rpc) {
        Rpc saved = rpcService.save(rpc);
        pushRpcMsgToRuleEngine(tenantId, saved);
        return saved;
    }

    /**
     * 更新 RPC 状态与可选响应体；记录已删除则仅打警告。
     */
    @Override
    public void save(TenantId tenantId, RpcId rpcId, RpcStatus newStatus, JsonNode response) {
        Rpc foundRpc = rpcService.findById(tenantId, rpcId);
        if (foundRpc != null) {
            foundRpc.setStatus(newStatus);
            if (response != null) {
                foundRpc.setResponse(response);
            }
            Rpc saved = rpcService.save(foundRpc);
            pushRpcMsgToRuleEngine(tenantId, saved);
        } else {
            log.warn("[{}] Failed to update RPC status because RPC was already deleted", rpcId);
        }
    }

    private void pushRpcMsgToRuleEngine(TenantId tenantId, Rpc rpc) {
        TbMsg msg = TbMsg.newMsg()
                .type(TbMsgType.valueOf("RPC_" + rpc.getStatus().name()))
                .originator(rpc.getDeviceId())
                .copyMetaData(TbMsgMetaData.EMPTY)
                .data(JacksonUtil.toString(rpc))
                .build();
        tbClusterService.pushMsgToRuleEngine(tenantId, rpc.getDeviceId(), msg, null);
    }

    /**
     * 按 ID 查询 RPC。
     */
    @Override
    public Rpc findRpcById(TenantId tenantId, RpcId rpcId) {
        return rpcService.findById(tenantId, rpcId);
    }

    /**
     * 按设备与状态分页查询 RPC。
     */
    @Override
    public PageData<Rpc> findAllByDeviceIdAndStatus(TenantId tenantId, DeviceId deviceId, RpcStatus rpcStatus, PageLink pageLink) {
        return rpcService.findAllByDeviceIdAndStatus(tenantId, deviceId, rpcStatus, pageLink);
    }

}
