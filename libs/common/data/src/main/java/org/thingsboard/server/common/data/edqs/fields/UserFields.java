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
public class UserFields extends AbstractEntityFields {

    private String firstName;
    private String lastName;
    private String email;
    private String phone;
    private String additionalInfo;

    @Override
    public String getName() {
        return super.getEmail();
    }

    public UserFields(UUID id, long createdTime, UUID tenantId, UUID customerId,
                      Long version, String firstName, String lastName, String email,
                      String phone, JsonNode additionalInfo) {
        super(id, createdTime, tenantId, customerId, version);
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.phone = phone;
        this.additionalInfo = getText(additionalInfo);
    }
}
