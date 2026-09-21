package com.jnks.iot.server.service.rpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.RpcId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.rpc.Rpc;
import com.jnks.iot.server.common.data.rpc.RpcStatus;

public interface JnksIotRpcService {

    Rpc save(TenantId tenantId, Rpc rpc);

    void save(TenantId tenantId, RpcId rpcId, RpcStatus newStatus, JsonNode response);

    Rpc findRpcById(TenantId tenantId, RpcId rpcId);

    PageData<Rpc> findAllByDeviceIdAndStatus(TenantId tenantId, DeviceId deviceId, RpcStatus rpcStatus, PageLink pageLink);
}
