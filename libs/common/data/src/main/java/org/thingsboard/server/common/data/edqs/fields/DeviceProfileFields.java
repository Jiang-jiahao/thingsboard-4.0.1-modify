package org.thingsboard.server.common.data.edqs.fields;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.thingsboard.server.common.data.DeviceProfileType;

import java.util.UUID;

@Data
@NoArgsConstructor
@SuperBuilder
public class DeviceProfileFields extends AbstractEntityFields {

    private String type;
    private boolean isDefault;

    public DeviceProfileFields(UUID id, long createdTime, UUID tenantId, String name, Long version, DeviceProfileType type, boolean isDefault) {
        super(id, createdTime, tenantId, null, name, version);
        this.type = type.name();
        this.isDefault = isDefault;
    }
}
