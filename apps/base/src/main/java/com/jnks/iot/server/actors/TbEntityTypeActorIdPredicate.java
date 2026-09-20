package com.jnks.iot.server.actors;

import lombok.RequiredArgsConstructor;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.EntityId;

import java.util.function.Predicate;

@RequiredArgsConstructor
public class TbEntityTypeActorIdPredicate implements Predicate<TbActorId> {

    private final EntityType entityType;

    @Override
    public boolean test(TbActorId actorId) {
        return actorId instanceof TbEntityActorId && testEntityId(((TbEntityActorId) actorId).getEntityId());
    }

    protected boolean testEntityId(EntityId entityId) {
        return entityId.getEntityType().equals(entityType);
    }
}
