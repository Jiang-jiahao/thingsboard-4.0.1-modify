package org.thingsboard.server.dao.entity;

import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.HasId;
import org.thingsboard.server.common.data.id.NameLabelAndCustomerDetails;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.query.EntityCountQuery;
import org.thingsboard.server.common.data.query.EntityData;
import org.thingsboard.server.common.data.query.EntityDataQuery;

import java.util.Optional;

public interface EntityService {

    Optional<String> fetchEntityName(TenantId tenantId, EntityId entityId);

    Optional<String> fetchEntityLabel(TenantId tenantId, EntityId entityId);

    Optional<CustomerId> fetchEntityCustomerId(TenantId tenantId, EntityId entityId);

    Optional<HasId<?>> fetchEntity(TenantId tenantId, EntityId entityId);

    Optional<NameLabelAndCustomerDetails> fetchNameLabelAndCustomerDetails(TenantId tenantId, EntityId entityId);

    long countEntitiesByQuery(TenantId tenantId, CustomerId customerId, EntityCountQuery query);

    PageData<EntityData> findEntityDataByQuery(TenantId tenantId, CustomerId customerId, EntityDataQuery query);
}
