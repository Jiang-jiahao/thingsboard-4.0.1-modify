package com.jnks.iot.server.actors;

import lombok.Getter;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.EntityId;

import java.util.Objects;

public class JnksIotCalculatedFieldEntityActorId implements JnksIotActorId {

    @Getter
    private final EntityId entityId;

    public JnksIotCalculatedFieldEntityActorId(EntityId entityId) {
        this.entityId = entityId;
    }

    @Override
    public String toString() {
        return entityId.getEntityType() + "|" + entityId.getId();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JnksIotCalculatedFieldEntityActorId that = (JnksIotCalculatedFieldEntityActorId) o;
        return entityId.equals(that.entityId);
    }

    @Override
    public int hashCode() {
        // Magic number to ensure that the hash does not match with the hash of other actor id - (JnksIotEntityActorId)
        return 42 + Objects.hash(entityId);
    }

    @Override
    public EntityType getEntityType() {
        return entityId.getEntityType();
    }
}
