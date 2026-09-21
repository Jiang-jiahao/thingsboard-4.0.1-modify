package com.jnks.iot.server.actors;

import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.msg.JnksIotActorMsg;

import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

public interface JnksIotActorCtx extends JnksIotActorRef {

    JnksIotActorId getSelf();

    JnksIotActorRef getParentRef();

    void tell(JnksIotActorId target, JnksIotActorMsg msg);

    void stop(JnksIotActorId target);

    JnksIotActorRef getOrCreateChildActor(JnksIotActorId actorId, Supplier<String> dispatcher, Supplier<JnksIotActorCreator> creator, Supplier<Boolean> createCondition);

    void broadcastToChildren(JnksIotActorMsg msg);

    void broadcastToChildren(JnksIotActorMsg msg, boolean highPriority);

    void broadcastToChildrenByType(JnksIotActorMsg msg, EntityType entityType);

    void broadcastToChildren(JnksIotActorMsg msg, Predicate<JnksIotActorId> childFilter);

    List<JnksIotActorId> filterChildren(Predicate<JnksIotActorId> childFilter);
}
