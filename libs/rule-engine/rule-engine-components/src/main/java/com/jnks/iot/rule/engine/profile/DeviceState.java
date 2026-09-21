package com.jnks.iot.rule.engine.profile;

import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.profile.state.PersistedAlarmState;
import com.jnks.iot.rule.engine.profile.state.PersistedDeviceState;
import com.jnks.iot.server.common.adaptor.JsonConverter;
import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.DataConstants;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.alarm.Alarm;
import com.jnks.iot.server.common.data.device.profile.AlarmConditionFilterKey;
import com.jnks.iot.server.common.data.device.profile.AlarmConditionKeyType;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileAlarm;
import com.jnks.iot.server.common.data.exception.ApiUsageLimitsExceededException;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;
import com.jnks.iot.server.common.data.kv.KvEntry;
import com.jnks.iot.server.common.data.kv.TsKvEntry;
import com.jnks.iot.server.common.data.query.EntityKey;
import com.jnks.iot.server.common.data.query.EntityKeyType;
import com.jnks.iot.server.common.data.rule.RuleNodeState;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.dao.sql.query.EntityKeyMapping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.ACTIVITY_EVENT;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.ALARM_ACK;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.ALARM_CLEAR;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.ALARM_DELETE;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.ATTRIBUTES_DELETED;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.ATTRIBUTES_UPDATED;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.ENTITY_ASSIGNED;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.ENTITY_UNASSIGNED;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.INACTIVITY_EVENT;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.POST_ATTRIBUTES_REQUEST;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.POST_TELEMETRY_REQUEST;

@Slf4j
class DeviceState {

    private final boolean persistState;
    private final DeviceId deviceId;
    private final ProfileState deviceProfile;
    private RuleNodeState state;
    private PersistedDeviceState pds;
    private DataSnapshot latestValues;
    private final ConcurrentMap<String, AlarmState> alarmStates = new ConcurrentHashMap<>();
    private final DynamicPredicateValueCtx dynamicPredicateValueCtx;

    DeviceState(JnksIotContext ctx, JnksIotDeviceProfileNodeConfiguration config, DeviceId deviceId, ProfileState deviceProfile, RuleNodeState state) {
        this.persistState = config.isPersistAlarmRulesState();
        this.deviceId = deviceId;
        this.deviceProfile = deviceProfile;

        this.dynamicPredicateValueCtx = new DynamicPredicateValueCtxImpl(ctx.getTenantId(), deviceId, ctx);

        if (config.isPersistAlarmRulesState()) {
            if (state != null) {
                this.state = state;
            } else {
                this.state = ctx.findRuleNodeStateForEntity(deviceId);
            }
            if (this.state != null) {
                pds = JacksonUtil.fromString(this.state.getStateData(), PersistedDeviceState.class);
            } else {
                this.state = new RuleNodeState();
                this.state.setRuleNodeId(ctx.getSelfId());
                this.state.setEntityId(deviceId);
                pds = new PersistedDeviceState();
                pds.setAlarmStates(new HashMap<>());
            }
        }
        if (pds != null) {
            for (DeviceProfileAlarm alarm : deviceProfile.getAlarmSettings()) {
                alarmStates.computeIfAbsent(alarm.getId(),
                        a -> new AlarmState(deviceProfile, deviceId, alarm, getOrInitPersistedAlarmState(alarm), dynamicPredicateValueCtx));
            }
        }
    }

    public void updateProfile(JnksIotContext ctx, DeviceProfile deviceProfile) throws ExecutionException, InterruptedException {
        Set<AlarmConditionFilterKey> oldKeys = Set.copyOf(this.deviceProfile.getEntityKeys());
        this.deviceProfile.updateDeviceProfile(deviceProfile);
        if (latestValues != null) {
            Set<AlarmConditionFilterKey> keysToFetch = new HashSet<>(this.deviceProfile.getEntityKeys());
            keysToFetch.removeAll(oldKeys);
            if (!keysToFetch.isEmpty()) {
                addEntityKeysToSnapshot(ctx, deviceId, keysToFetch, latestValues);
            }
        }
        Set<String> newAlarmStateIds = this.deviceProfile.getAlarmSettings().stream().map(DeviceProfileAlarm::getId).collect(Collectors.toSet());
        alarmStates.keySet().removeIf(id -> !newAlarmStateIds.contains(id));
        for (DeviceProfileAlarm alarm : this.deviceProfile.getAlarmSettings()) {
            if (alarmStates.containsKey(alarm.getId())) {
                alarmStates.get(alarm.getId()).updateState(alarm, getOrInitPersistedAlarmState(alarm));
            } else {
                alarmStates.putIfAbsent(alarm.getId(), new AlarmState(this.deviceProfile, deviceId, alarm, getOrInitPersistedAlarmState(alarm), dynamicPredicateValueCtx));
            }
        }
    }

    public void harvestAlarms(JnksIotContext ctx, long ts) throws ExecutionException, InterruptedException {
        log.debug("[{}] Going to harvest alarms: {}", ctx.getSelfId(), ts);
        boolean stateChanged = false;
        for (AlarmState state : alarmStates.values()) {
            stateChanged |= state.process(ctx, ts);
        }
        if (persistState && stateChanged) {
            state.setStateData(JacksonUtil.toString(pds));
            state = ctx.saveRuleNodeState(state);
        }
    }

    public void process(JnksIotContext ctx, JnksIotMsg msg) throws ExecutionException, InterruptedException {
        if (latestValues == null) {
            latestValues = fetchLatestValues(ctx, deviceId);
        }
        boolean stateChanged = false;
        if (msg.isTypeOf(POST_TELEMETRY_REQUEST)) {
            stateChanged = processTelemetry(ctx, msg);
        } else if (msg.isTypeOf(POST_ATTRIBUTES_REQUEST)) {
            stateChanged = processAttributesUpdateRequest(ctx, msg);
        } else if (msg.isTypeOneOf(ACTIVITY_EVENT, INACTIVITY_EVENT)) {
            stateChanged = processDeviceActivityEvent(ctx, msg);
        } else if (msg.isTypeOf(ATTRIBUTES_UPDATED)) {
            stateChanged = processAttributesUpdateNotification(ctx, msg);
        } else if (msg.isTypeOf(ATTRIBUTES_DELETED)) {
            stateChanged = processAttributesDeleteNotification(ctx, msg);
        } else if (msg.isTypeOf(ALARM_CLEAR)) {
            stateChanged = processAlarmClearNotification(ctx, msg);
        } else if (msg.isTypeOf(ALARM_ACK)) {
            processAlarmAckNotification(ctx, msg);
        } else if (msg.isTypeOf(ALARM_DELETE)) {
            processAlarmDeleteNotification(ctx, msg);
        } else {
            if (msg.isTypeOneOf(ENTITY_ASSIGNED, ENTITY_UNASSIGNED)) {
                dynamicPredicateValueCtx.resetCustomer();
            }
            ctx.tellSuccess(msg);
        }
        if (persistState && stateChanged) {
            state.setStateData(JacksonUtil.toString(pds));
            state = ctx.saveRuleNodeState(state);
        }
    }

    private boolean processDeviceActivityEvent(JnksIotContext ctx, JnksIotMsg msg) throws ExecutionException, InterruptedException {
        String scope = msg.getMetaData().getValue(DataConstants.SCOPE);
        if (StringUtils.isEmpty(scope)) {
            return processTelemetry(ctx, msg);
        } else {
            return processAttributes(ctx, msg, scope);
        }
    }

    private boolean processAlarmClearNotification(JnksIotContext ctx, JnksIotMsg msg) {
        boolean stateChanged = false;
        Alarm alarmNf = JacksonUtil.fromString(msg.getData(), Alarm.class);
        for (DeviceProfileAlarm alarm : deviceProfile.getAlarmSettings()) {
            AlarmState alarmState = alarmStates.computeIfAbsent(alarm.getId(),
                    a -> new AlarmState(this.deviceProfile, deviceId, alarm, getOrInitPersistedAlarmState(alarm), dynamicPredicateValueCtx));
            stateChanged |= alarmState.processAlarmClear(ctx, alarmNf);
        }
        ctx.tellSuccess(msg);
        return stateChanged;
    }

    private void processAlarmAckNotification(JnksIotContext ctx, JnksIotMsg msg) {
        Alarm alarmNf = JacksonUtil.fromString(msg.getData(), Alarm.class);
        for (DeviceProfileAlarm alarm : deviceProfile.getAlarmSettings()) {
            AlarmState alarmState = alarmStates.computeIfAbsent(alarm.getId(),
                    a -> new AlarmState(this.deviceProfile, deviceId, alarm, getOrInitPersistedAlarmState(alarm), dynamicPredicateValueCtx));
            alarmState.processAckAlarm(alarmNf);
        }
        ctx.tellSuccess(msg);
    }

    private void processAlarmDeleteNotification(JnksIotContext ctx, JnksIotMsg msg) {
        Alarm alarm = JacksonUtil.fromString(msg.getData(), Alarm.class);
        alarmStates.values().removeIf(alarmState -> alarmState.getCurrentAlarm() != null
                && alarmState.getCurrentAlarm().getId().equals(alarm.getId()));
        ctx.tellSuccess(msg);
    }

    private boolean processAttributesUpdateNotification(JnksIotContext ctx, JnksIotMsg msg) throws ExecutionException, InterruptedException {
        String scope = msg.getMetaData().getValue(DataConstants.SCOPE);
        if (StringUtils.isEmpty(scope)) {
            scope = DataConstants.CLIENT_SCOPE;
        }
        return processAttributes(ctx, msg, scope);
    }

    private boolean processAttributesDeleteNotification(JnksIotContext ctx, JnksIotMsg msg) throws ExecutionException, InterruptedException {
        boolean stateChanged = false;
        List<String> keys = new ArrayList<>();
        JsonParser.parseString(msg.getData()).getAsJsonObject().get("attributes").getAsJsonArray().forEach(e -> keys.add(e.getAsString()));
        String scope = msg.getMetaData().getValue(DataConstants.SCOPE);
        if (StringUtils.isEmpty(scope)) {
            scope = DataConstants.CLIENT_SCOPE;
        }
        if (!keys.isEmpty()) {
            EntityKeyType keyType = getKeyTypeFromScope(scope);
            Set<AlarmConditionFilterKey> removedKeys = keys.stream().map(key -> new EntityKey(keyType, key))
                    .peek(latestValues::removeValue)
                    .map(DataSnapshot::toConditionKey).collect(Collectors.toSet());
            SnapshotUpdate update = new SnapshotUpdate(AlarmConditionKeyType.ATTRIBUTE, removedKeys);

            for (DeviceProfileAlarm alarm : deviceProfile.getAlarmSettings()) {
                AlarmState alarmState = alarmStates.computeIfAbsent(alarm.getId(),
                        a -> new AlarmState(this.deviceProfile, deviceId, alarm, getOrInitPersistedAlarmState(alarm), dynamicPredicateValueCtx));
                stateChanged |= alarmState.process(ctx, msg, latestValues, update);
            }
        }
        ctx.tellSuccess(msg);
        return stateChanged;
    }

    protected boolean processAttributesUpdateRequest(JnksIotContext ctx, JnksIotMsg msg) throws ExecutionException, InterruptedException {
        return processAttributes(ctx, msg, DataConstants.CLIENT_SCOPE);
    }

    private boolean processAttributes(JnksIotContext ctx, JnksIotMsg msg, String scope) throws ExecutionException, InterruptedException {
        boolean stateChanged = false;
        Set<AttributeKvEntry> attributes = JsonConverter.convertToAttributes(JsonParser.parseString(msg.getData()));
        if (!attributes.isEmpty()) {
            SnapshotUpdate update = merge(latestValues, attributes, scope);
            for (DeviceProfileAlarm alarm : deviceProfile.getAlarmSettings()) {
                AlarmState alarmState = alarmStates.computeIfAbsent(alarm.getId(),
                        a -> new AlarmState(this.deviceProfile, deviceId, alarm, getOrInitPersistedAlarmState(alarm), dynamicPredicateValueCtx));
                stateChanged |= alarmState.process(ctx, msg, latestValues, update);
            }
        }
        ctx.tellSuccess(msg);
        return stateChanged;
    }

    protected boolean processTelemetry(JnksIotContext ctx, JnksIotMsg msg) throws ExecutionException, InterruptedException {
        boolean stateChanged = false;
        Map<Long, List<KvEntry>> tsKvMap = JsonConverter.convertToSortedTelemetry(JsonParser.parseString(msg.getData()), msg.getMetaDataTs());
        // iterate over data by ts (ASC order).
        for (Map.Entry<Long, List<KvEntry>> entry : tsKvMap.entrySet()) {
            Long ts = entry.getKey();
            List<KvEntry> data = entry.getValue();
            SnapshotUpdate update = merge(latestValues, ts, data);
            if (update.hasUpdate()) {
                for (DeviceProfileAlarm alarm : deviceProfile.getAlarmSettings()) {
                    AlarmState alarmState = alarmStates.computeIfAbsent(alarm.getId(),
                            a -> new AlarmState(this.deviceProfile, deviceId, alarm, getOrInitPersistedAlarmState(alarm), dynamicPredicateValueCtx));
                    try {
                        stateChanged |= alarmState.process(ctx, msg, latestValues, update);
                    } catch (ApiUsageLimitsExceededException e) {
                        alarmStates.remove(alarm.getId());
                        throw e;
                    }
                }
            }
        }
        ctx.tellSuccess(msg);
        return stateChanged;
    }

    private SnapshotUpdate merge(DataSnapshot latestValues, Long newTs, List<KvEntry> data) {
        Set<AlarmConditionFilterKey> keys = new HashSet<>();
        for (KvEntry entry : data) {
            AlarmConditionFilterKey entityKey = new AlarmConditionFilterKey(AlarmConditionKeyType.TIME_SERIES, entry.getKey());
            if (latestValues.putValue(entityKey, newTs, toEntityValue(entry))) {
                keys.add(entityKey);
            }
        }
        latestValues.setTs(newTs);
        return new SnapshotUpdate(AlarmConditionKeyType.TIME_SERIES, keys);
    }

    private SnapshotUpdate merge(DataSnapshot latestValues, Set<AttributeKvEntry> attributes, String scope) {
        long newTs = 0;
        Set<AlarmConditionFilterKey> keys = new HashSet<>();
        for (AttributeKvEntry entry : attributes) {
            newTs = Math.max(newTs, entry.getLastUpdateTs());
            AlarmConditionFilterKey entityKey = new AlarmConditionFilterKey(AlarmConditionKeyType.ATTRIBUTE, entry.getKey());
            if (latestValues.putValue(entityKey, newTs, toEntityValue(entry))) {
                keys.add(entityKey);
            }
        }
        latestValues.setTs(newTs);
        return new SnapshotUpdate(AlarmConditionKeyType.ATTRIBUTE, keys);
    }

    private static EntityKeyType getKeyTypeFromScope(String scope) {
        switch (scope) {
            case DataConstants.CLIENT_SCOPE:
                return EntityKeyType.CLIENT_ATTRIBUTE;
            case DataConstants.SHARED_SCOPE:
                return EntityKeyType.SHARED_ATTRIBUTE;
            case DataConstants.SERVER_SCOPE:
                return EntityKeyType.SERVER_ATTRIBUTE;
        }
        return EntityKeyType.ATTRIBUTE;
    }

    private DataSnapshot fetchLatestValues(JnksIotContext ctx, EntityId originator) throws ExecutionException, InterruptedException {
        Set<AlarmConditionFilterKey> entityKeysToFetch = deviceProfile.getEntityKeys();
        DataSnapshot result = new DataSnapshot(entityKeysToFetch);
        addEntityKeysToSnapshot(ctx, originator, entityKeysToFetch, result);
        return result;
    }

    private void addEntityKeysToSnapshot(JnksIotContext ctx, EntityId originator, Set<AlarmConditionFilterKey> entityKeysToFetch, DataSnapshot result) throws InterruptedException, ExecutionException {
        Set<String> attributeKeys = new HashSet<>();
        Set<String> latestTsKeys = new HashSet<>();

        Device device = null;
        for (AlarmConditionFilterKey entityKey : entityKeysToFetch) {
            String key = entityKey.getKey();
            switch (entityKey.getType()) {
                case ATTRIBUTE:
                    attributeKeys.add(key);
                    break;
                case TIME_SERIES:
                    latestTsKeys.add(key);
                    break;
                case ENTITY_FIELD:
                    if (device == null) {
                        device = ctx.getDeviceService().findDeviceById(ctx.getTenantId(), new DeviceId(originator.getId()));
                    }
                    if (device != null) {
                        switch (key) {
                            case EntityKeyMapping.NAME:
                                result.putValue(entityKey, device.getCreatedTime(), EntityKeyValue.fromString(device.getName()));
                                break;
                            case EntityKeyMapping.TYPE:
                                result.putValue(entityKey, device.getCreatedTime(), EntityKeyValue.fromString(device.getType()));
                                break;
                            case EntityKeyMapping.CREATED_TIME:
                                result.putValue(entityKey, device.getCreatedTime(), EntityKeyValue.fromLong(device.getCreatedTime()));
                                break;
                            case EntityKeyMapping.LABEL:
                                result.putValue(entityKey, device.getCreatedTime(), EntityKeyValue.fromString(device.getLabel()));
                                break;
                        }
                    }
                    break;
            }
        }

        if (!latestTsKeys.isEmpty()) {
            List<TsKvEntry> data = ctx.getTimeseriesService().findLatest(ctx.getTenantId(), originator, latestTsKeys).get();
            for (TsKvEntry entry : data) {
                if (entry.getValue() != null) {
                    result.putValue(new AlarmConditionFilterKey(AlarmConditionKeyType.TIME_SERIES, entry.getKey()), entry.getTs(), toEntityValue(entry));
                }
            }
        }
        if (!attributeKeys.isEmpty()) {
            addToSnapshot(result, ctx.getAttributesService().find(ctx.getTenantId(), originator, AttributeScope.CLIENT_SCOPE, attributeKeys).get());
            addToSnapshot(result, ctx.getAttributesService().find(ctx.getTenantId(), originator, AttributeScope.SHARED_SCOPE, attributeKeys).get());
            addToSnapshot(result, ctx.getAttributesService().find(ctx.getTenantId(), originator, AttributeScope.SERVER_SCOPE, attributeKeys).get());
        }
    }

    private void addToSnapshot(DataSnapshot snapshot, List<AttributeKvEntry> data) {
        for (AttributeKvEntry entry : data) {
            if (entry.getValue() != null) {
                EntityKeyValue value = toEntityValue(entry);
                snapshot.putValue(new AlarmConditionFilterKey(AlarmConditionKeyType.ATTRIBUTE, entry.getKey()), entry.getLastUpdateTs(), value);
            }
        }
    }

    public static EntityKeyValue toEntityValue(KvEntry entry) {
        switch (entry.getDataType()) {
            case STRING:
                return EntityKeyValue.fromString(entry.getStrValue().get());
            case LONG:
                return EntityKeyValue.fromLong(entry.getLongValue().get());
            case DOUBLE:
                return EntityKeyValue.fromDouble(entry.getDoubleValue().get());
            case BOOLEAN:
                return EntityKeyValue.fromBool(entry.getBooleanValue().get());
            case JSON:
                return EntityKeyValue.fromJson(entry.getJsonValue().get());
            default:
                throw new RuntimeException("Can't parse entry: " + entry.getDataType());
        }
    }

    public DeviceProfileId getProfileId() {
        return deviceProfile.getProfileId();
    }

    private PersistedAlarmState getOrInitPersistedAlarmState(DeviceProfileAlarm alarm) {
        if (pds != null) {
            PersistedAlarmState alarmState = pds.getAlarmStates().get(alarm.getId());
            if (alarmState == null) {
                alarmState = new PersistedAlarmState();
                alarmState.setCreateRuleStates(new HashMap<>());
                pds.getAlarmStates().put(alarm.getId(), alarmState);
            }
            return alarmState;
        } else {
            return null;
        }
    }

}
