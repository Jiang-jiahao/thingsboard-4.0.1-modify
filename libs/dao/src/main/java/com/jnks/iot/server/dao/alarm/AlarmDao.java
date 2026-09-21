package com.jnks.iot.server.dao.alarm;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.server.common.data.EntitySubtype;
import com.jnks.iot.server.common.data.alarm.Alarm;
import com.jnks.iot.server.common.data.alarm.AlarmApiCallResult;
import com.jnks.iot.server.common.data.alarm.AlarmCreateOrUpdateActiveRequest;
import com.jnks.iot.server.common.data.alarm.AlarmInfo;
import com.jnks.iot.server.common.data.alarm.AlarmQuery;
import com.jnks.iot.server.common.data.alarm.AlarmQueryV2;
import com.jnks.iot.server.common.data.alarm.AlarmSeverity;
import com.jnks.iot.server.common.data.alarm.AlarmStatusFilter;
import com.jnks.iot.server.common.data.alarm.AlarmUpdateRequest;
import com.jnks.iot.server.common.data.alarm.EntityAlarm;
import com.jnks.iot.server.common.data.id.AlarmId;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.query.AlarmCountQuery;
import com.jnks.iot.server.common.data.query.AlarmData;
import com.jnks.iot.server.common.data.query.AlarmDataQuery;
import com.jnks.iot.server.common.data.query.OriginatorAlarmFilter;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.dao.Dao;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Created by ashvayka on 11.05.17.
 */
public interface AlarmDao extends Dao<Alarm> {

    Alarm findLatestByOriginatorAndType(TenantId tenantId, EntityId originator, String type);

    Alarm findLatestActiveByOriginatorAndType(TenantId tenantId, EntityId originator, String type);

    ListenableFuture<Alarm> findLatestByOriginatorAndTypeAsync(TenantId tenantId, EntityId originator, String type);

    Alarm findAlarmById(TenantId tenantId, UUID key);

    ListenableFuture<Alarm> findAlarmByIdAsync(TenantId tenantId, UUID key);

    AlarmInfo findAlarmInfoById(TenantId tenantId, UUID key);

    Alarm save(TenantId tenantId, Alarm alarm);

    PageData<AlarmInfo> findAlarms(TenantId tenantId, AlarmQuery query);

    PageData<AlarmInfo> findCustomerAlarms(TenantId tenantId, CustomerId customerId, AlarmQuery query);

    PageData<AlarmInfo> findAlarmsV2(TenantId tenantId, AlarmQueryV2 query);

    PageData<AlarmInfo> findCustomerAlarmsV2(TenantId tenantId, CustomerId customerId, AlarmQueryV2 query);

    PageData<AlarmData> findAlarmDataByQueryForEntities(TenantId tenantId, AlarmDataQuery query, Collection<EntityId> orderedEntityIds);

    Set<AlarmSeverity> findAlarmSeverities(TenantId tenantId, EntityId entityId, AlarmStatusFilter asf, String assigneeId);

    PageData<AlarmId> findAlarmsIdsByEndTsBeforeAndTenantId(Long time, TenantId tenantId, PageLink pageLink);

    PageData<JnksIotPair<UUID, Long>> findAlarmIdsByAssigneeId(TenantId tenantId, UserId userId, long createdTimeOffset, AlarmId idOffset, int limit);

    PageData<JnksIotPair<UUID, Long>> findAlarmIdsByOriginatorId(TenantId tenantId, EntityId originatorId, long createdTimeOffset, AlarmId idOffset, int limit);

    void createEntityAlarmRecord(EntityAlarm entityAlarm);

    List<EntityAlarm> findEntityAlarmRecords(TenantId tenantId, AlarmId id);

    List<EntityAlarm> findEntityAlarmRecordsByEntityId(TenantId tenantId, EntityId entityId);

    int deleteEntityAlarmRecords(TenantId tenantId, EntityId entityId);

    void deleteEntityAlarmRecordsByTenantId(TenantId tenantId);

    AlarmApiCallResult createOrUpdateActiveAlarm(AlarmCreateOrUpdateActiveRequest request, boolean alarmCreationEnabled);

    AlarmApiCallResult updateAlarm(AlarmUpdateRequest request);

    AlarmApiCallResult acknowledgeAlarm(TenantId tenantId, AlarmId id, long ackTs);

    AlarmApiCallResult clearAlarm(TenantId tenantId, AlarmId alarmId, long clearTs, JsonNode details);

    AlarmApiCallResult assignAlarm(TenantId tenantId, AlarmId alarmId, UserId assigneeId, long assignTime);

    AlarmApiCallResult unassignAlarm(TenantId tenantId, AlarmId alarmId, long unassignTime);

    long countAlarmsByQuery(TenantId tenantId, CustomerId customerId, AlarmCountQuery query, Collection<EntityId> orderedEntityIds);

    PageData<EntitySubtype> findTenantAlarmTypes(UUID tenantId, PageLink pageLink);

    boolean removeAlarmTypesIfNoAlarmsPresent(UUID tenantId, Set<String> types);

    List<UUID> findActiveOriginatorAlarms(TenantId tenantId, OriginatorAlarmFilter originatorAlarmFilter, int limit);

}
