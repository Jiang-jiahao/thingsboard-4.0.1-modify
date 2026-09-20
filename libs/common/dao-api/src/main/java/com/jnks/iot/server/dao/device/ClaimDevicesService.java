package com.jnks.iot.server.dao.device;

import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.device.claim.ClaimResult;
import com.jnks.iot.server.dao.device.claim.ReclaimResult;

public interface ClaimDevicesService {

    ListenableFuture<Void> registerClaimingInfo(TenantId tenantId, DeviceId deviceId, String secretKey, long durationMs);

    ListenableFuture<ClaimResult> claimDevice(Device device, CustomerId customerId, String secretKey);

    ListenableFuture<ReclaimResult> reClaimDevice(TenantId tenantId, Device device);

}
