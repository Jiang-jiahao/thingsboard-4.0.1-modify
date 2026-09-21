package com.jnks.iot.server.dao.attributes;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.tuple.Pair;
import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.dao.model.sql.AttributeKvEntity;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * @author Andrew Shvayka
 */
public interface AttributesDao {

    Optional<AttributeKvEntry> find(TenantId tenantId, EntityId entityId, AttributeScope attributeScope, String attributeKey);

    List<AttributeKvEntry> find(TenantId tenantId, EntityId entityId, AttributeScope attributeScope, Collection<String> attributeKey);

    List<AttributeKvEntry> findAll(TenantId tenantId, EntityId entityId, AttributeScope attributeScope);

    ListenableFuture<Long> save(TenantId tenantId, EntityId entityId, AttributeScope attributeScope, AttributeKvEntry attribute);

    List<ListenableFuture<String>> removeAll(TenantId tenantId, EntityId entityId, AttributeScope attributeScope, List<String> keys);

    List<ListenableFuture<JnksIotPair<String, Long>>> removeAllWithVersions(TenantId tenantId, EntityId entityId, AttributeScope attributeScope, List<String> keys);

    List<AttributeKvEntity> findNextBatch(UUID entityId, int attributeType, int attributeKey, int batchSize);

    List<String> findAllKeysByDeviceProfileId(TenantId tenantId, DeviceProfileId deviceProfileId);

    List<String> findAllKeysByEntityIds(TenantId tenantId, List<EntityId> entityIds);

    List<String> findAllKeysByEntityIdsAndAttributeType(TenantId tenantId, List<EntityId> entityIds, String attributeType);

    List<Pair<AttributeScope, String>> removeAllByEntityId(TenantId tenantId, EntityId entityId);

}
