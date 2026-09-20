package com.jnks.iot.server.common.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.EntityIdFactory;
import com.jnks.iot.server.common.data.id.HasId;

import java.util.UUID;

@Schema
@Data
public class EntityInfo implements HasId<EntityId>, HasName {

    @Schema(description = "JSON object with the entity Id. ")
    private final EntityId id;
    @Schema(description = "Entity Name")
    private final String name;

    @JsonCreator
    public EntityInfo(@JsonProperty("id") EntityId id, @JsonProperty("name") String name) {
        this.id = id;
        this.name = name;
    }

    public EntityInfo(UUID uuid, String entityType, String name) {
        this.id = EntityIdFactory.getByTypeAndUuid(entityType, uuid);
        this.name = name;
    }

}
