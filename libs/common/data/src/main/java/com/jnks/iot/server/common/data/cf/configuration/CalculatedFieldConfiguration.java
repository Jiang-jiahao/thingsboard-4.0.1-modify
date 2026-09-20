package com.jnks.iot.server.common.data.cf.configuration;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.jnks.iot.server.common.data.cf.CalculatedFieldLink;
import com.jnks.iot.server.common.data.cf.CalculatedFieldType;
import com.jnks.iot.server.common.data.id.CalculatedFieldId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;

import java.util.List;
import java.util.Map;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type"
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = SimpleCalculatedFieldConfiguration.class, name = "SIMPLE"),
        @JsonSubTypes.Type(value = ScriptCalculatedFieldConfiguration.class, name = "SCRIPT")
})
public interface CalculatedFieldConfiguration {

    @JsonIgnore
    CalculatedFieldType getType();

    Map<String, Argument> getArguments();

    String getExpression();

    void setExpression(String expression);

    Output getOutput();

    @JsonIgnore
    List<EntityId> getReferencedEntities();

    List<CalculatedFieldLink> buildCalculatedFieldLinks(TenantId tenantId, EntityId cfEntityId, CalculatedFieldId calculatedFieldId);

    CalculatedFieldLink buildCalculatedFieldLink(TenantId tenantId, EntityId referencedEntityId, CalculatedFieldId calculatedFieldId);

}
