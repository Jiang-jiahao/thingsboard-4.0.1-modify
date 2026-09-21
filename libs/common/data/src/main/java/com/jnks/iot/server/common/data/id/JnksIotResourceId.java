package com.jnks.iot.server.common.data.id;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import com.jnks.iot.server.common.data.EntityType;

import java.util.UUID;

public class JnksIotResourceId extends UUIDBased implements EntityId {

    private static final long serialVersionUID = 1L;

    @JsonCreator
    public JnksIotResourceId(@JsonProperty("id") UUID id) {
        super(id);
    }

    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = "string", example = "JNKS_IOT_RESOURCE", allowableValues = "JNKS_IOT_RESOURCE")
    @Override
    public EntityType getEntityType() {
        return EntityType.JNKS_IOT_RESOURCE;
    }
}
