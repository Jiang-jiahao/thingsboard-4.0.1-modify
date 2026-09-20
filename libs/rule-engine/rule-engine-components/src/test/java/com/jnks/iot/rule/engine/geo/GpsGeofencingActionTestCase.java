package com.jnks.iot.rule.engine.geo;

import lombok.Data;
import com.jnks.iot.server.common.data.id.EntityId;

import java.util.HashMap;
import java.util.Map;

@Data
public class GpsGeofencingActionTestCase {

    private EntityId entityId;
    private Map<EntityId, EntityGeofencingState> entityStates;
    private boolean msgInside;
    private boolean reportPresenceStatusOnEachMessage;

    public GpsGeofencingActionTestCase(EntityId entityId, boolean msgInside, boolean reportPresenceStatusOnEachMessage, EntityGeofencingState entityGeofencingState) {
        this.entityId = entityId;
        this.msgInside = msgInside;
        this.reportPresenceStatusOnEachMessage = reportPresenceStatusOnEachMessage;
        this.entityStates = new HashMap<>();
        this.entityStates.put(entityId, entityGeofencingState);
    }
}
