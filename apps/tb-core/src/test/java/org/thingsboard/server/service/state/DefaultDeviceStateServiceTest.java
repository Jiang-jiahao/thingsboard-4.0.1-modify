/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.service.state;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.thingsboard.server.cluster.TbClusterService;
import org.thingsboard.server.common.data.DeviceIdInfo;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.query.EntityData;
import org.thingsboard.server.common.data.query.EntityKeyType;
import org.thingsboard.server.common.data.query.TsValue;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.TbMsgMetaData;
import org.thingsboard.server.common.msg.notification.NotificationRuleProcessor;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.common.stats.TbApiUsageReportClient;
import org.thingsboard.server.dao.attributes.AttributesService;
import org.thingsboard.server.dao.device.DeviceService;
import org.thingsboard.server.dao.sql.query.EntityQueryRepository;
import org.thingsboard.server.dao.timeseries.TimeseriesService;
import org.thingsboard.server.dao.util.DbTypeInfoComponent;
import org.thingsboard.server.queue.TbQueueCallback;
import org.thingsboard.server.queue.discovery.PartitionService;
import org.thingsboard.server.queue.discovery.TbServiceInfoProvider;
import org.thingsboard.server.service.state.constants.DefaultDeviceStateConstants;
import org.thingsboard.server.service.telemetry.TelemetrySubscriptionService;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class DefaultDeviceStateServiceTest {

    private static final long NOW = 1_700_000_000_000L;
    private static final long TIMEOUT_MS = 600_000L;

    @Mock DeviceService deviceService;
    @Mock AttributesService attributesService;
    @Mock TimeseriesService tsService;
    @Mock TbClusterService clusterService;
    @Mock PartitionService partitionService;
    @Mock TbServiceInfoProvider serviceInfoProvider;
    @Mock EntityQueryRepository entityQueryRepository;
    @Mock DbTypeInfoComponent dbTypeInfoComponent;
    @Mock TbApiUsageReportClient apiUsageReportClient;
    @Mock NotificationRuleProcessor notificationRuleProcessor;
    @Mock TelemetrySubscriptionService tsSubService;

    DefaultDeviceStateService service;
    TenantId tenantId;
    DeviceId deviceId;
    TopicPartitionInfo tpi;

    @BeforeEach
    void setUp() {
        tenantId = TenantId.fromUUID(UUID.randomUUID());
        deviceId = new DeviceId(UUID.randomUUID());
        tpi = new TopicPartitionInfo("tb_core", tenantId, 0, true);
        service = new FixedClockDeviceStateService(deviceService, attributesService, tsService, clusterService,
                partitionService, serviceInfoProvider, entityQueryRepository, dbTypeInfoComponent,
                apiUsageReportClient, notificationRuleProcessor, NOW);
        service.setDefaultInactivityTimeoutMs(TIMEOUT_MS);
        service.setPersistToTelemetry(false);
        ReflectionTestUtils.setField(service, "tsSubService", tsSubService);
        lenient().when(partitionService.resolve(any(), any(), any())).thenReturn(tpi);
        lenient().when(serviceInfoProvider.isService(ServiceType.TB_TRANSPORT)).thenReturn(true);
        partitionedEntities().put(tpi, ConcurrentHashMap.newKeySet());
        lenient().doNothing().when(clusterService).pushMsgToRuleEngine(any(TenantId.class), any(EntityId.class), any(TbMsg.class), nullable(TbQueueCallback.class));
        lenient().doNothing().when(notificationRuleProcessor).process(any());
        lenient().doNothing().when(tsSubService).saveAttributes(any());
    }

    @Test
    void partitionInit_doesNotReviveInactiveDeviceWhenLastActivityStillInTimeoutWindow() {
        DeviceStateData data = stateData(false, NOW - 10_000L, NOW - 9_000L);
        deviceStates().put(deviceId, data);

        service.checkAndUpdateState(deviceId, data, true);

        assertThat(data.getState().isActive()).isFalse();
    }

    @Test
    void partitionInit_doesNotReviveInactiveDeviceEvenIfInactivityAlarmWasNotLoaded() {
        // 实体查询没带回 inactivityAlarmTime 时，旧逻辑会按 lastActivityTime 窗口把设备拉回活跃。
        DeviceStateData data = stateData(false, NOW - 10_000L, 0L);
        deviceStates().put(deviceId, data);

        service.checkAndUpdateState(deviceId, data, true);

        assertThat(data.getState().isActive()).isFalse();
    }

    @Test
    void partitionInit_localTransportRestartForcesStoredActiveDeviceInactive() {
        DeviceStateData data = stateData(true, NOW - 10_000L, 0L);
        deviceStates().put(deviceId, data);

        service.checkAndUpdateState(deviceId, data, true);

        assertThat(data.getState().isActive()).isFalse();
        assertThat(data.getState().getLastInactivityAlarmTime()).isGreaterThan(data.getState().getLastActivityTime());
    }

    @Test
    void partitionInit_coreOnlyNodeKeepsRecentlyActiveDeviceUntilTimeout() {
        given(serviceInfoProvider.isService(ServiceType.TB_TRANSPORT)).willReturn(false);
        DeviceStateData data = stateData(true, NOW - 10_000L, 0L);
        deviceStates().put(deviceId, data);

        service.checkAndUpdateState(deviceId, data, true);

        assertThat(data.getState().isActive()).isTrue();
    }

    @Test
    void staleActivityAfterExplicitInactivityIsIgnored() {
        DeviceStateData data = stateData(false, NOW - 20_000L, NOW - 5_000L);
        deviceStates().put(deviceId, data);

        service.onDeviceActivity(tenantId, deviceId, NOW - 15_000L);

        assertThat(data.getState().isActive()).isFalse();
    }

    @Test
    void newActivityAfterExplicitInactivityMarksDeviceActive() {
        DeviceStateData data = stateData(false, NOW - 20_000L, NOW - 5_000L);
        deviceStates().put(deviceId, data);

        service.onDeviceActivity(tenantId, deviceId, NOW);

        assertThat(data.getState().isActive()).isTrue();
        assertThat(data.getState().getLastActivityTime()).isEqualTo(NOW);
    }

    @Test
    void expiredActiveDeviceBecomesInactiveOnPartitionInit() {
        given(serviceInfoProvider.isService(ServiceType.TB_TRANSPORT)).willReturn(false);
        DeviceStateData data = stateData(true, NOW - TIMEOUT_MS - 1, 0L);
        deviceStates().put(deviceId, data);

        service.checkAndUpdateState(deviceId, data, true);

        assertThat(data.getState().isActive()).isFalse();
    }

    @Test
    void partitionInit_entityQueryWithoutInactivityAlarmDoesNotReviveInactiveDevice() {
        DeviceStateData data = fromEntityQuery("false", NOW - 10_000L, null);
        deviceStates().put(deviceId, data);

        service.checkAndUpdateState(deviceId, data, true);

        assertThat(data.getState().isActive()).isFalse();
        assertThat(data.getState().getLastInactivityAlarmTime()).isZero();
        verify(tsSubService, never()).saveAttributes(any());
    }

    @Test
    void partitionInit_stuckActiveAfterPreviousFalseReviveIsForcedInactiveOnMonolith() {
        // 上次错误回补会把 active=true 且 inactivityAlarmTime=0 写入库，之后既没有连接也不会超时。
        DeviceStateData data = fromEntityQuery("true", NOW - 10_000L, "0");
        deviceStates().put(deviceId, data);

        service.checkAndUpdateState(deviceId, data, true);

        assertThat(data.getState().isActive()).isFalse();
        assertThat(data.getState().getLastInactivityAlarmTime()).isGreaterThan(data.getState().getLastActivityTime());
        verify(tsSubService, atLeastOnce()).saveAttributes(any());
    }

    @Test
    void timeoutUpdateAfterPartitionInitDoesNotReviveExplicitlyInactiveDevice() {
        DeviceStateData data = stateData(false, NOW - 10_000L, NOW - 1_000L);
        deviceStates().put(deviceId, data);

        service.onDeviceInactivityTimeoutUpdate(tenantId, deviceId, TIMEOUT_MS);

        assertThat(data.getState().isActive()).isFalse();
    }

    @SuppressWarnings("unchecked")
    private ConcurrentMap<DeviceId, DeviceStateData> deviceStates() {
        return (ConcurrentMap<DeviceId, DeviceStateData>) ReflectionTestUtils.getField(service, "deviceStates");
    }

    @SuppressWarnings("unchecked")
    private ConcurrentMap<TopicPartitionInfo, Set<DeviceId>> partitionedEntities() {
        return (ConcurrentMap<TopicPartitionInfo, Set<DeviceId>>) ReflectionTestUtils.getField(service, "partitionedEntities");
    }

    private DeviceStateData stateData(boolean active, long lastActivityTime, long lastInactivityAlarmTime) {
        return DeviceStateData.builder()
                .tenantId(tenantId)
                .deviceId(deviceId)
                .deviceCreationTime(NOW - 86_400_000L)
                .metaData(new TbMsgMetaData())
                .state(DeviceState.builder()
                        .active(active)
                        .lastActivityTime(lastActivityTime)
                        .lastInactivityAlarmTime(lastInactivityAlarmTime)
                        .inactivityTimeout(TIMEOUT_MS)
                        .build())
                .build();
    }

    private DeviceStateData fromEntityQuery(String active, long lastActivityTime, String inactivityAlarmTime) {
        Map<String, TsValue> attrs = new HashMap<>();
        attrs.put(DefaultDeviceStateConstants.ACTIVITY_STATE, new TsValue(NOW, active));
        attrs.put(DefaultDeviceStateConstants.LAST_ACTIVITY_TIME, new TsValue(NOW, Long.toString(lastActivityTime)));
        if (inactivityAlarmTime != null) {
            attrs.put(DefaultDeviceStateConstants.INACTIVITY_ALARM_TIME, new TsValue(NOW, inactivityAlarmTime));
        }
        EntityData entityData = new EntityData();
        entityData.setEntityId(deviceId);
        entityData.setLatest(Map.of(EntityKeyType.SERVER_ATTRIBUTE, attrs));
        return service.toDeviceStateData(entityData, new DeviceIdInfo(tenantId.getId(), null, deviceId.getId()));
    }

    private static final class FixedClockDeviceStateService extends DefaultDeviceStateService {
        private final long now;

        private FixedClockDeviceStateService(DeviceService deviceService, AttributesService attributesService,
                                             TimeseriesService tsService, TbClusterService clusterService,
                                             PartitionService partitionService, TbServiceInfoProvider serviceInfoProvider,
                                             EntityQueryRepository entityQueryRepository, DbTypeInfoComponent dbTypeInfoComponent,
                                             TbApiUsageReportClient apiUsageReportClient,
                                             NotificationRuleProcessor notificationRuleProcessor, long now) {
            super(deviceService, attributesService, tsService, clusterService, partitionService, serviceInfoProvider,
                    entityQueryRepository, dbTypeInfoComponent, apiUsageReportClient, notificationRuleProcessor);
            this.now = now;
        }

        @Override
        long getCurrentTimeMillis() {
            return now;
        }
    }
}
