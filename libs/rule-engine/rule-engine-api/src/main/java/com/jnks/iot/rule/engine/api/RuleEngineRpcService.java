package com.jnks.iot.rule.engine.api;

import com.jnks.iot.server.common.data.id.RpcId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.rpc.Rpc;
import com.jnks.iot.server.common.msg.TbMsg;

import java.util.UUID;
import java.util.function.Consumer;

/**
 * Created by ashvayka on 02.04.18.
 */
public interface RuleEngineRpcService {

    void sendRpcReplyToDevice(String serviceId, UUID sessionId, int requestId, String body);

    void sendRpcRequestToDevice(RuleEngineDeviceRpcRequest request, Consumer<RuleEngineDeviceRpcResponse> consumer);

    void sendRestApiCallReply(String serviceId, UUID requestId, TbMsg msg);

    Rpc findRpcById(TenantId tenantId, RpcId id);
}
