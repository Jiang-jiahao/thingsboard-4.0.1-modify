package com.jnks.iot.server.edqs.data;

import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.edqs.fields.EntityFields;
import com.jnks.iot.server.common.data.permission.QueryContext;
import com.jnks.iot.server.common.data.query.EntityKeyType;
import com.jnks.iot.server.common.data.edqs.DataPoint;
import com.jnks.iot.server.edqs.query.DataKey;
import com.jnks.iot.server.edqs.repo.TenantRepo;

import java.util.UUID;

public interface EntityData<T extends EntityFields> {

    UUID getId();

    EntityType getEntityType();

    UUID getCustomerId();

    void setCustomerId(UUID customerId);

    void setRepo(TenantRepo repo);

    T getFields();

    void setFields(T fields);

    DataPoint getAttr(Integer keyId, EntityKeyType entityKeyType);

    boolean putAttr(Integer keyId, AttributeScope scope, DataPoint value);

    boolean removeAttr(Integer keyId, AttributeScope scope);

    DataPoint getTs(Integer keyId);

    boolean putTs(Integer keyId, DataPoint value);

    boolean removeTs(Integer keyId);

    String getOwnerName();

    String getOwnerType();

    DataPoint getDataPoint(DataKey key, QueryContext queryContext);

    String getField(String name);

    boolean isEmpty();

}
