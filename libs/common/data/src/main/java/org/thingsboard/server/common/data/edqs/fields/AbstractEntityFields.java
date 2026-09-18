package org.thingsboard.server.common.data.edqs.fields;

import lombok.Data;
import lombok.experimental.SuperBuilder;
import org.thingsboard.server.common.data.id.CustomerId;

import java.util.UUID;

@Data
@SuperBuilder
public class AbstractEntityFields implements EntityFields {

    private UUID id;
    private long createdTime;
    private UUID tenantId;
    private UUID customerId;
    private String name;
    private Long version;

    public AbstractEntityFields(UUID id, long createdTime, UUID tenantId, UUID customerId, String name, Long version) {
        this.id = id;
        this.createdTime = createdTime;
        this.tenantId = tenantId;
        this.customerId = (customerId != null && customerId != CustomerId.NULL_UUID) ? customerId : null;
        this.name = name;
        this.version = version;
    }

    public AbstractEntityFields() {
    }

    public AbstractEntityFields(UUID id, long createdTime, UUID tenantId, String name, Long version) {
        this(id, createdTime, tenantId, null, name, version);
    }

    public AbstractEntityFields(UUID id, long createdTime, UUID tenantId, UUID customerId, Long version) {
        this(id, createdTime, tenantId, customerId, null, version);

    }

    public AbstractEntityFields(UUID id, long createdTime, String name, Long version) {
        this(id, createdTime, null, name, version);
    }


    public AbstractEntityFields(UUID id, long createdTime, UUID tenantId) {
        this(id, createdTime, tenantId, null, null, null);
    }

}
