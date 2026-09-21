package com.jnks.iot.rule.engine.profile;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

@Slf4j
public class DynamicPredicateValueCtxImpl implements DynamicPredicateValueCtx {
    private final TenantId tenantId;
    private CustomerId customerId;
    private final DeviceId deviceId;
    private final JnksIotContext ctx;

    public DynamicPredicateValueCtxImpl(TenantId tenantId, DeviceId deviceId, JnksIotContext ctx) {
        this.tenantId = tenantId;
        this.deviceId = deviceId;
        this.ctx = ctx;
        resetCustomer();
    }

    @Override
    public EntityKeyValue getTenantValue(String key) {
        return getValue(tenantId, key);
    }

    @Override
    public EntityKeyValue getCustomerValue(String key) {
        return customerId == null || customerId.isNullUid() ? null : getValue(customerId, key);
    }

    @Override
    public void resetCustomer() {
        Device device = ctx.getDeviceService().findDeviceById(tenantId, deviceId);
        if (device != null) {
            this.customerId = device.getCustomerId();
        }
    }

    private EntityKeyValue getValue(EntityId entityId, String key) {
        try {
            Optional<AttributeKvEntry> entry = ctx.getAttributesService().find(tenantId, entityId, AttributeScope.SERVER_SCOPE, key).get();
            if (entry.isPresent()) {
                return DeviceState.toEntityValue(entry.get());
            }
        } catch (InterruptedException | ExecutionException e) {
            log.warn("Failed to get attribute by key: {} for {}: [{}]", key, entityId.getEntityType(), entityId.getId());
        }
        return null;
    }
}
