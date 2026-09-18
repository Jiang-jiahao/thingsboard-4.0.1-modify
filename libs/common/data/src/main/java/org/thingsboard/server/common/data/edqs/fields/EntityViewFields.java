package org.thingsboard.server.common.data.edqs.fields;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.UUID;

import static org.thingsboard.server.common.data.edqs.fields.FieldsUtil.getText;

@Data
@NoArgsConstructor
@SuperBuilder
public class EntityViewFields extends AbstractEntityFields {

    private String type;
    private String additionalInfo;

    public EntityViewFields(UUID id, long createdTime, UUID tenantId, UUID customerId, String name, String type, JsonNode additionalInfo, Long version) {
        super(id, createdTime, tenantId, customerId, name, version);
        this.type = type;
        this.additionalInfo = getText(additionalInfo);
    }
}
