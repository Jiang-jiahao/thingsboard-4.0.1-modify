package com.jnks.iot.server.actors;

import com.jnks.iot.server.common.data.EntityType;

import java.util.Objects;

public class JnksIotStringActorId implements JnksIotActorId {

    private final String id;

    public JnksIotStringActorId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JnksIotStringActorId that = (JnksIotStringActorId) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public EntityType getEntityType() {
        return null;
    }
}
