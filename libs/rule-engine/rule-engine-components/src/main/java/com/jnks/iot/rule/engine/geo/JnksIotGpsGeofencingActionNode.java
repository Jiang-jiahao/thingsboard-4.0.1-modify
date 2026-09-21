package com.jnks.iot.rule.engine.geo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;
import com.jnks.iot.server.common.data.kv.BaseAttributeKvEntry;
import com.jnks.iot.server.common.data.kv.StringDataEntry;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.jnks.iot.rule.engine.util.GpsGeofencingEvents.ENTERED;
import static com.jnks.iot.rule.engine.util.GpsGeofencingEvents.INSIDE;
import static com.jnks.iot.rule.engine.util.GpsGeofencingEvents.LEFT;
import static com.jnks.iot.rule.engine.util.GpsGeofencingEvents.OUTSIDE;

/**
 * Created by ashvayka on 19.01.18.
 */
@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "gps geofencing events",
        version = 1,
        configClazz = JnksIotGpsGeofencingActionNodeConfiguration.class,
        relationTypes = {"Success", "Entered", "Left", "Inside", "Outside"},
        nodeDescription = "Produces incoming messages using GPS based geofencing",
        nodeDetails = "Extracts latitude and longitude parameters from incoming message and returns different events based on configuration parameters. " +
                "<br><br>" +
                "If an object with coordinates extracted from incoming message enters the geofence, sends a message with the type <code>Entered</code>. " +
                "If an object leaves the geofence, sends a message with the type <code>Left</code>. " +
                "If the presence monitoring strategy <b>\"On first message\"</b> is selected, sends messages via rule node connection type <code>Inside</code> or <code>Outside</code> only the first time the geofencing and duration conditions are satisfied; otherwise sends messages via rule node connection type <code>Success</code>. " +
                "If the presence monitoring strategy <b>\"On each message\"</b> is selected, sends messages via rule node connection type <code>Inside</code> or <code>Outside</code> every time the geofencing condition is satisfied. " +
                "<br><br>" +
                "Output connections: <code>Entered</code>, <code>Left</code>, <code>Inside</code>, <code>Outside</code>, <code>Success</code>",
        configDirective = "jnksIotActionNodeGpsGeofencingConfig"
)
public class JnksIotGpsGeofencingActionNode extends AbstractGeofencingNode<JnksIotGpsGeofencingActionNodeConfiguration> {

    private static final String REPORT_PRESENCE_STATUS_ON_EACH_MESSAGE = "reportPresenceStatusOnEachMessage";
    private final Map<EntityId, EntityGeofencingState> entityStates = new HashMap<>();
    private final Gson gson = new Gson();

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) throws JnksIotNodeException {
        boolean matches = checkMatches(msg);
        long ts = System.currentTimeMillis();

        EntityGeofencingState entityState = entityStates.computeIfAbsent(msg.getOriginator(), key -> {
            try {
                Optional<AttributeKvEntry> entry = ctx.getAttributesService()
                        .find(ctx.getTenantId(), msg.getOriginator(), AttributeScope.SERVER_SCOPE, ctx.getServiceId())
                        .get(1, TimeUnit.MINUTES);
                if (entry.isPresent()) {
                    JsonObject element = JsonParser.parseString(entry.get().getValueAsString()).getAsJsonObject();
                    return new EntityGeofencingState(element.get("inside").getAsBoolean(), element.get("stateSwitchTime").getAsLong(), element.get("stayed").getAsBoolean());
                } else {
                    return new EntityGeofencingState(false, 0L, false);
                }
            } catch (InterruptedException | TimeoutException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        if (entityState.getStateSwitchTime() == 0L || entityState.isInside() != matches) {
            switchState(ctx, msg.getOriginator(), entityState, matches, ts);
            ctx.tellNext(msg, matches ? ENTERED : LEFT);
            return;
        }

        if (config.isReportPresenceStatusOnEachMessage()) {
            ctx.tellNext(msg, entityState.isInside() ? INSIDE : OUTSIDE);
            return;
        }

        if (entityState.isStayed()) {
            ctx.tellSuccess(msg);
            return;
        }

        long stayTime = ts - entityState.getStateSwitchTime();
        if (stayTime > (entityState.isInside() ?
                TimeUnit.valueOf(config.getMinInsideDurationTimeUnit()).toMillis(config.getMinInsideDuration()) :
                TimeUnit.valueOf(config.getMinOutsideDurationTimeUnit()).toMillis(config.getMinOutsideDuration()))) {
            setStaid(ctx, msg.getOriginator(), entityState);
            ctx.tellNext(msg, entityState.isInside() ? INSIDE : OUTSIDE);
            return;
        }

        ctx.tellSuccess(msg);
    }

    private void switchState(JnksIotContext ctx, EntityId entityId, EntityGeofencingState entityState, boolean matches, long ts) {
        entityState.setInside(matches);
        entityState.setStateSwitchTime(ts);
        entityState.setStayed(false);
        persist(ctx, entityId, entityState);
    }

    private void setStaid(JnksIotContext ctx, EntityId entityId, EntityGeofencingState entityState) {
        entityState.setStayed(true);
        persist(ctx, entityId, entityState);
    }

    private void persist(JnksIotContext ctx, EntityId entityId, EntityGeofencingState entityState) {
        JsonObject object = new JsonObject();
        object.addProperty("inside", entityState.isInside());
        object.addProperty("stateSwitchTime", entityState.getStateSwitchTime());
        object.addProperty("stayed", entityState.isStayed());
        AttributeKvEntry entry = new BaseAttributeKvEntry(new StringDataEntry(ctx.getServiceId(), gson.toJson(object)), System.currentTimeMillis());
        List<AttributeKvEntry> attributeKvEntryList = Collections.singletonList(entry);
        ctx.getAttributesService().save(ctx.getTenantId(), entityId, AttributeScope.SERVER_SCOPE, attributeKvEntryList);
    }

    @Override
    protected Class<JnksIotGpsGeofencingActionNodeConfiguration> getConfigClazz() {
        return JnksIotGpsGeofencingActionNodeConfiguration.class;
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        boolean hasChanges = false;
        if (fromVersion == 0) {
            if (!oldConfiguration.has(REPORT_PRESENCE_STATUS_ON_EACH_MESSAGE)) {
                hasChanges = true;
                ((ObjectNode) oldConfiguration).put(REPORT_PRESENCE_STATUS_ON_EACH_MESSAGE, false);
            }
        }
        return new JnksIotPair<>(hasChanges, oldConfiguration);
    }

}
