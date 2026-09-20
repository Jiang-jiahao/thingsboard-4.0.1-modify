package com.jnks.iot.server.dao.attributes;

import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * @author Andrew Shvayka
 */
public interface AttributesService {

    ListenableFuture<Optional<AttributeKvEntry>> find(TenantId tenantId, EntityId entityId, AttributeScope scope, String attributeKey);

    ListenableFuture<List<AttributeKvEntry>> find(TenantId tenantId, EntityId entityId, AttributeScope scope, Collection<String> attributeKeys);

    ListenableFuture<List<AttributeKvEntry>> findAll(TenantId tenantId, EntityId entityId, AttributeScope scope);

    ListenableFuture<List<Long>> save(TenantId tenantId, EntityId entityId, AttributeScope scope, List<AttributeKvEntry> attributes);

    ListenableFuture<Long> save(TenantId tenantId, EntityId entityId, AttributeScope scope, AttributeKvEntry attribute);

    ListenableFuture<List<String>> removeAll(TenantId tenantId, EntityId entityId, AttributeScope scope, List<String> attributeKeys);

    List<String> findAllKeysByDeviceProfileId(TenantId tenantId, DeviceProfileId deviceProfileId);

    List<String> findAllKeysByEntityIds(TenantId tenantId, List<EntityId> entityIds);

    List<String> findAllKeysByEntityIds(TenantId tenantId, List<EntityId> entityIds, String scope);

    int removeAllByEntityId(TenantId tenantId, EntityId entityId);

}
