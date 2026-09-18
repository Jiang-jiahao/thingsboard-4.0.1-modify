package org.thingsboard.server.common.data.edqs.fields;

import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.UUID;

@NoArgsConstructor
@SuperBuilder
public class WidgetsBundleFields extends AbstractEntityFields {

    public WidgetsBundleFields(UUID id, long createdTime, UUID tenantId, String name, Long version) {
        super(id, createdTime, tenantId, name, version);
    }
}
