package org.thingsboard.server.common.data.edqs.fields;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.UUID;

@Data
@NoArgsConstructor
@SuperBuilder
public class AssetProfileFields extends AbstractEntityFields {

    private boolean isDefault;

    public AssetProfileFields(UUID id, long createdTime, UUID tenantId, String name, Long version, boolean isDefault) {
        super(id, createdTime, tenantId, null, name, version);
        this.isDefault = isDefault;
    }
}
