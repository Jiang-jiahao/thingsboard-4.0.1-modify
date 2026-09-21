package com.jnks.iot.server.service.rpc;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.cluster.JnksIotClusterService;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.RpcId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.rpc.Rpc;
import com.jnks.iot.server.common.data.rpc.RpcStatus;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;
import com.jnks.iot.server.dao.rpc.RpcService;
/**
 * 持久化 RPC 实体服务。
 * <p>
 * 保存或更新 RPC 状态后，向规则引擎推送 {@code RPC_{STATUS}} 消息，供规则链感知调用进度。
 *
 * @see JnksIotRpcService
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class DefaultJnksIotRpcService implements JnksIotRpcService {
    private final RpcService rpcService;
    private final JnksIotClusterService jnksIotClusterService;

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
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.valueOf("RPC_" + rpc.getStatus().name()))
                .originator(rpc.getDeviceId())
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JacksonUtil.toString(rpc))
                .build();
        jnksIotClusterService.pushMsgToRuleEngine(tenantId, rpc.getDeviceId(), msg, null);
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
