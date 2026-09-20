package com.jnks.iot.server.dao.rpc;

import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.RpcId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.rpc.Rpc;
import com.jnks.iot.server.common.data.rpc.RpcStatus;
import com.jnks.iot.server.dao.entity.EntityDaoService;

public interface RpcService extends EntityDaoService {

    Rpc save(Rpc rpc);

    void deleteRpc(TenantId tenantId, RpcId id);

    void deleteAllRpcByTenantId(TenantId tenantId);

    Rpc findById(TenantId tenantId, RpcId id);

    ListenableFuture<Rpc> findRpcByIdAsync(TenantId tenantId, RpcId id);

    PageData<Rpc> findAllByDeviceId(TenantId tenantId, DeviceId deviceId, PageLink pageLink);

    PageData<Rpc> findAllByDeviceIdAndStatus(TenantId tenantId, DeviceId deviceId, RpcStatus rpcStatus, PageLink pageLink);
}
