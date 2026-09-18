package org.thingsboard.server.common.data.edqs.fields;

import lombok.NoArgsConstructor;

import java.util.UUID;

@NoArgsConstructor
public class GenericFields extends AbstractEntityFields {

    public GenericFields(UUID id, long createdTime, UUID tenantId, String name, Long version) {
        super(id, createdTime, tenantId, name, version);
    }

}
