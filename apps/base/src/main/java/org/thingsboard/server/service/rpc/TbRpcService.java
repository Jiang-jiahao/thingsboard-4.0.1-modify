package org.thingsboard.server.service.rpc;

import com.fasterxml.jackson.databind.JsonNode;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.RpcId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.rpc.Rpc;
import org.thingsboard.server.common.data.rpc.RpcStatus;

public interface TbRpcService {

    Rpc save(TenantId tenantId, Rpc rpc);

    void save(TenantId tenantId, RpcId rpcId, RpcStatus newStatus, JsonNode response);

    Rpc findRpcById(TenantId tenantId, RpcId rpcId);

    PageData<Rpc> findAllByDeviceIdAndStatus(TenantId tenantId, DeviceId deviceId, RpcStatus rpcStatus, PageLink pageLink);
}
