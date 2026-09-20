package com.jnks.iot.server.dao.rpc;

import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.rpc.Rpc;
import com.jnks.iot.server.common.data.rpc.RpcStatus;
import com.jnks.iot.server.dao.Dao;

public interface RpcDao extends Dao<Rpc> {
    PageData<Rpc> findAllByDeviceId(TenantId tenantId, DeviceId deviceId, PageLink pageLink);

    PageData<Rpc> findAllByDeviceIdAndStatus(TenantId tenantId, DeviceId deviceId, RpcStatus rpcStatus, PageLink pageLink);

    PageData<Rpc> findAllRpcByTenantId(TenantId tenantId, PageLink pageLink);

    int deleteOutdatedRpcByTenantId(TenantId tenantId, Long expirationTime);

}
