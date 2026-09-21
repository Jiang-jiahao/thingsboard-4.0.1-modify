package com.jnks.iot.server.actors;

import lombok.Getter;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.EntityId;

import java.util.Objects;

public class JnksIotEntityActorId implements JnksIotActorId {

    @Getter
    private final EntityId entityId;

    public JnksIotEntityActorId(EntityId entityId) {
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
        JnksIotEntityActorId that = (JnksIotEntityActorId) o;
        return entityId.equals(that.entityId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityId);
    }

    @Override
    public EntityType getEntityType() {
        return entityId.getEntityType();
    }
}
