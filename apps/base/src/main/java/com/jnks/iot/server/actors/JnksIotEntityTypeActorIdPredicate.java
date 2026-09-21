package com.jnks.iot.server.actors;

import lombok.RequiredArgsConstructor;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.EntityId;

import java.util.function.Predicate;

@RequiredArgsConstructor
public class JnksIotEntityTypeActorIdPredicate implements Predicate<JnksIotActorId> {

    private final EntityType entityType;

    @Override
    public boolean test(JnksIotActorId actorId) {
        return actorId instanceof JnksIotEntityActorId && testEntityId(((JnksIotEntityActorId) actorId).getEntityId());
    }

    protected boolean testEntityId(EntityId entityId) {
        return entityId.getEntityType().equals(entityType);
    }
}
