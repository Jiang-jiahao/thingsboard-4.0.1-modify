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
package org.thingsboard.server.service.apiusage;

import com.google.common.util.concurrent.ListenableFuture;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.thingsboard.rule.engine.api.MailService;
import org.thingsboard.rule.engine.api.TimeseriesSaveRequest;
import org.thingsboard.server.common.data.ApiFeature;
import org.thingsboard.server.common.data.ApiUsageRecordKey;
import org.thingsboard.server.common.data.ApiUsageRecordState;
import org.thingsboard.server.common.data.ApiUsageState;
import org.thingsboard.server.common.data.ApiUsageStateValue;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.Tenant;
import org.thingsboard.server.common.data.TenantProfile;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.ApiUsageStateId;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.id.TenantProfileId;
import org.thingsboard.server.common.data.kv.BasicTsKvEntry;
import org.thingsboard.server.common.data.kv.LongDataEntry;
import org.thingsboard.server.common.data.kv.StringDataEntry;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.data.notification.rule.trigger.ApiUsageLimitTrigger;
import org.thingsboard.server.common.data.page.PageDataIterable;
import org.thingsboard.server.common.data.tenant.profile.TenantProfileConfiguration;
import org.thingsboard.server.common.data.tenant.profile.TenantProfileData;
import org.thingsboard.server.common.msg.notification.NotificationRuleProcessor;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.common.msg.queue.TbCallback;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.common.msg.tools.SchedulerUtils;
import org.thingsboard.server.common.util.ProtoUtils;
import org.thingsboard.server.dao.tenant.TbTenantProfileCache;
import org.thingsboard.server.dao.tenant.TenantService;
import org.thingsboard.server.dao.timeseries.TimeseriesService;
import org.thingsboard.server.dao.usagerecord.ApiUsageStateService;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.gen.transport.TransportProtos.ToUsageStatsServiceMsg;
import org.thingsboard.server.gen.transport.TransportProtos.UsageStatsKVProto;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;
import org.thingsboard.server.queue.discovery.PartitionService;
import org.thingsboard.server.service.apiusage.BaseApiUsageState.StatsCalculationResult;
import org.thingsboard.server.service.executors.DbCallbackExecutorService;
import org.thingsboard.server.service.mail.MailExecutorService;
import org.thingsboard.server.service.partition.AbstractPartitionBasedService;
import org.thingsboard.server.service.telemetry.InternalTelemetryService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class DefaultTbApiUsageStateService extends AbstractPartitionBasedService<EntityId> implements TbApiUsageStateService {

    public static final String HOURLY = "Hourly";

    private final PartitionService partitionService;
    private final TenantService tenantService;
    private final TimeseriesService tsService;
    private final ApiUsageStateService apiUsageStateService;
    private final TbTenantProfileCache tenantProfileCache;
    private final MailService mailService;
    private final NotificationRuleProcessor notificationRuleProcessor;
    private final DbCallbackExecutorService dbExecutor;
    private final MailExecutorService mailExecutor;

    @Lazy
    @Autowired
    private InternalTelemetryService tsWsService;

    // Entities that should be processed on this server
    final Map<EntityId, BaseApiUsageState> myUsageStates = new ConcurrentHashMap<>();
    // Entities that should be processed on other servers
    final Map<EntityId, ApiUsageState> otherUsageStates = new ConcurrentHashMap<>();

    final Set<EntityId> deletedEntities = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Value("${usage.stats.report.enabled:true}")
    private boolean enabled;

    @Value("${usage.stats.check.cycle:60000}")
    private long nextCycleCheckInterval;

    @Value("${usage.stats.gauge_report_interval:180000}")
    private long gaugeReportInterval;

    private final Lock updateLock = new ReentrantLock();

    @PostConstruct
    public void init() {
        super.init();
        if (enabled) {
            log.info("Starting api usage service.");
            scheduledExecutor.scheduleAtFixedRate(this::checkStartOfNextCycle, nextCycleCheckInterval, nextCycleCheckInterval, TimeUnit.MILLISECONDS);
            log.info("Started api usage service.");
        }
    }

    @Override
    protected String getServiceName() {
        return "API Usage";
    }

    @Override
    protected String getSchedulerExecutorName() {
        return "api-usage-scheduled";
    }

    /**
     * core服务接收并处理来自其他微服务（如规则引擎、传输层）上报的API使用数据
     * @param msgPack api使用数据
     * @param callback 回调函数
     */
    @Override
    public void process(TbProtoQueueMsg<ToUsageStatsServiceMsg> msgPack, TbCallback callback) {
        ToUsageStatsServiceMsg serviceMsg = msgPack.getValue();
        String serviceId = serviceMsg.getServiceId();

        List<TransportProtos.UsageStatsServiceMsg> msgs;

        //For backward compatibility, remove after release
        if (serviceMsg.getMsgsList().isEmpty()) {
            // 新版本的消息体，构建旧的消息体
            TransportProtos.UsageStatsServiceMsg oldMsg = TransportProtos.UsageStatsServiceMsg.newBuilder()
                    .setTenantIdMSB(serviceMsg.getTenantIdMSB())
                    .setTenantIdLSB(serviceMsg.getTenantIdLSB())
                    .setCustomerIdMSB(serviceMsg.getCustomerIdMSB())
                    .setCustomerIdLSB(serviceMsg.getCustomerIdLSB())
                    .setEntityIdMSB(serviceMsg.getEntityIdMSB())
                    .setEntityIdLSB(serviceMsg.getEntityIdLSB())
                    .addAllValues(serviceMsg.getValuesList())
                    .build();

            msgs = List.of(oldMsg);
        } else {
            // 表示是旧版本的消息
            msgs = serviceMsg.getMsgsList();
        }

        msgs.forEach(msg -> {
            TenantId tenantId = TenantId.fromUUID(new UUID(msg.getTenantIdMSB(), msg.getTenantIdLSB()));
            EntityId ownerId;
            // 判断是客户还是租户
            if (msg.getCustomerIdMSB() != 0 && msg.getCustomerIdLSB() != 0) {
                ownerId = new CustomerId(new UUID(msg.getCustomerIdMSB(), msg.getCustomerIdLSB()));
            } else {
                ownerId = tenantId;
            }
            // 处理实体的API使用统计数据
            processEntityUsageStats(tenantId, ownerId, msg.getValuesList(), serviceId);
        });
        callback.onSuccess();
    }

    /**
     * 1、获取对应的实体api使用统计对象并恢复对应数据（恢复的统计数据来源于数据库存储的数据）
     * 2、判断统计对象数据是否发生改变，如果发生改变，则更新数据库
     * @param tenantId 租户ID
     * @param ownerId 所有者ID（租户ID或客户ID）
     * @param values API使用统计键值对列表
     * @param serviceId 上报服务实例ID
     */
    private void processEntityUsageStats(TenantId tenantId, EntityId ownerId, List<UsageStatsKVProto> values, String serviceId) {
        // 跳过已删除的实体
        if (deletedEntities.contains(ownerId)) return;

        BaseApiUsageState usageState;
        // 需要更新的时序数据
        List<TsKvEntry> updatedEntries;
        Map<ApiFeature, ApiUsageStateValue> result;
        // 加锁确保状态更新的线程安全
        updateLock.lock();
        try {
            // 获取或创建对应实体的API使用统计对象
            usageState = getOrFetchState(tenantId, ownerId);
            long ts = usageState.getCurrentCycleTs();
            long hourTs = usageState.getCurrentHourTs();
            long newHourTs = SchedulerUtils.getStartOfCurrentHour();
            if (newHourTs != hourTs) {
                // 如果现在的小时和原来的不是同一个小时了，那么直接更新小时
                usageState.setHour(newHourTs);
            }
            if (log.isTraceEnabled()) {
                log.trace("[{}][{}] Processing usage stats from {} (currentCycleTs={}, currentHourTs={}): {}", tenantId, ownerId, serviceId, ts, newHourTs, values);
            }
            updatedEntries = new ArrayList<>(ApiUsageRecordKey.values().length);
            Set<ApiFeature> apiFeatures = new HashSet<>();
            for (UsageStatsKVProto statsItem : values) {
                ApiUsageRecordKey recordKey;

                //For backward compatibility, remove after release
                if (StringUtils.isNotEmpty(statsItem.getKey())) {
                    recordKey = ApiUsageRecordKey.valueOf(statsItem.getKey());
                } else {
                    recordKey = ProtoUtils.fromProto(statsItem.getRecordKey());
                }

                // 根据其他服务实例给定的kv，来计算新的统计值
                StatsCalculationResult calculationResult = usageState.calculate(recordKey, statsItem.getValue(), serviceId);
                if (calculationResult.isValueChanged()) {
                    // 如果周期性数据发生改变了，则添加到需要更新的时序数据中
                    long newValue = calculationResult.getNewValue();
                    updatedEntries.add(new BasicTsKvEntry(ts, new LongDataEntry(recordKey.getApiCountKey(), newValue)));
                }
                if (calculationResult.isHourlyValueChanged()) {
                    // 如果小时数据发生改变了，则添加到需要更新的时序数据中
                    long newHourlyValue = calculationResult.getNewHourlyValue();
                    updatedEntries.add(new BasicTsKvEntry(newHourTs, new LongDataEntry(recordKey.getApiCountKey() + HOURLY, newHourlyValue)));
                }
                if (recordKey.getApiFeature() != null) {
                    apiFeatures.add(recordKey.getApiFeature());
                }
            }
            // 检查租户状态是否需要更新（达到阈值）
            // 注意：系统租户（SYS_TENANT_ID）不做限制
            if (usageState.getEntityType() == EntityType.TENANT && !usageState.getEntityId().equals(TenantId.SYS_TENANT_ID)) {
                result = ((TenantApiUsageState) usageState).checkStateUpdatedDueToThreshold(apiFeatures);
            } else {
                result = Collections.emptyMap();
            }
        } finally {
            updateLock.unlock();
        }
        log.trace("[{}][{}] Saving new stats: {}", tenantId, ownerId, updatedEntries);
        // 更新数据库中的时序数据
        tsWsService.saveTimeseriesInternal(TimeseriesSaveRequest.builder()
                .tenantId(tenantId)
                .entityId(usageState.getApiUsageState().getId())
                .entries(updatedEntries)
                .build());
        // 如果有状态变更（如达到警告或限制阈值），发送通知
        if (!result.isEmpty()) {
            persistAndNotify(usageState, result);
        }
    }

    @Override
    public ApiUsageState getApiUsageState(TenantId tenantId) {
        TenantApiUsageState tenantState = (TenantApiUsageState) myUsageStates.get(tenantId);
        if (tenantState != null) {
            return tenantState.getApiUsageState();
        } else {
            ApiUsageState state = otherUsageStates.get(tenantId);
            if (state != null) {
                return state;
            } else {
                if (partitionService.resolve(ServiceType.TB_CORE, tenantId, tenantId).isMyPartition()) {
                    return getOrFetchState(tenantId, tenantId).getApiUsageState();
                } else {
                    state = otherUsageStates.get(tenantId);
                    if (state == null) {
                        state = apiUsageStateService.findTenantApiUsageState(tenantId);
                        if (state != null) {
                            otherUsageStates.put(tenantId, state);
                        }
                    }
                    return state;
                }
            }
        }
    }

    @Override
    public void onApiUsageStateUpdate(TenantId tenantId) {
        // 当api使用统计被更新的时候，从缓存中删除该租户的api使用统计
        otherUsageStates.remove(tenantId);
    }

    @Override
    public void onTenantProfileUpdate(TenantProfileId tenantProfileId) {
        log.info("[{}] On Tenant Profile Update", tenantProfileId);
        TenantProfile tenantProfile = tenantProfileCache.get(tenantProfileId);
        updateLock.lock();
        try {
            myUsageStates.values().stream()
                    .filter(state -> state.getEntityType() == EntityType.TENANT)
                    .map(state -> (TenantApiUsageState) state)
                    .forEach(state -> {
                        if (tenantProfile.getId().equals(state.getTenantProfileId())) {
                            updateTenantState(state, tenantProfile);
                        }
                    });
        } finally {
            updateLock.unlock();
        }
    }

    @Override
    public void onTenantUpdate(TenantId tenantId) {
        log.info("[{}] On Tenant Update.", tenantId);
        TenantProfile tenantProfile = tenantProfileCache.get(tenantId);
        updateLock.lock();
        try {
            TenantApiUsageState state = (TenantApiUsageState) myUsageStates.get(tenantId);
            if (state != null && !state.getTenantProfileId().equals(tenantProfile.getId())) {
                updateTenantState(state, tenantProfile);
            }
        } finally {
            updateLock.unlock();
        }
    }

    private void updateTenantState(TenantApiUsageState state, TenantProfile profile) {
        TenantProfileData oldProfileData = state.getTenantProfileData();
        state.setTenantProfileId(profile.getId());
        state.setTenantProfileData(profile.getProfileData());
        Map<ApiFeature, ApiUsageStateValue> result = state.checkStateUpdatedDueToThresholds();
        if (!result.isEmpty()) {
            persistAndNotify(state, result);
        }
        updateProfileThresholds(state.getTenantId(), state.getApiUsageState().getId(),
                oldProfileData.getConfiguration(), profile.getProfileData().getConfiguration());
    }

    /**
     * 将实体状态添加到本服务器的管理集合中
     * @param tpi 主题分区信息
     * @param state API使用状态
     */
    private void addEntityState(TopicPartitionInfo tpi, BaseApiUsageState state) {
        EntityId entityId = state.getEntityId();
        Set<EntityId> entityIds = partitionedEntities.get(tpi);
        if (entityIds != null) {
            entityIds.add(entityId);
            myUsageStates.put(entityId, state);
        } else {
            log.debug("[{}] belongs to external partition {}", entityId, tpi.getFullTopicName());
            throw new RuntimeException(entityId.getEntityType() + " belongs to external partition " + tpi.getFullTopicName() + "!");
        }
    }

    private void updateProfileThresholds(TenantId tenantId, ApiUsageStateId id,
                                         TenantProfileConfiguration oldData, TenantProfileConfiguration newData) {
        long ts = System.currentTimeMillis();
        List<TsKvEntry> profileThresholds = new ArrayList<>();
        for (ApiUsageRecordKey key : ApiUsageRecordKey.values()) {
            long newProfileThreshold = newData.getProfileThreshold(key);
            if (oldData == null || oldData.getProfileThreshold(key) != newProfileThreshold) {
                log.info("[{}] Updating profile threshold [{}]:[{}]", tenantId, key, newProfileThreshold);
                profileThresholds.add(new BasicTsKvEntry(ts, new LongDataEntry(key.getApiLimitKey(), newProfileThreshold)));
            }
        }
        if (!profileThresholds.isEmpty()) {
            tsWsService.saveTimeseriesInternal(TimeseriesSaveRequest.builder()
                    .tenantId(tenantId)
                    .entityId(id)
                    .entries(profileThresholds)
                    .build());
        }
    }

    public void onTenantDelete(TenantId tenantId) {
        deletedEntities.add(tenantId);
        myUsageStates.remove(tenantId);
        otherUsageStates.remove(tenantId);
    }

    @Override
    public void onCustomerDelete(CustomerId customerId) {
        deletedEntities.add(customerId);
        myUsageStates.remove(customerId);
    }

    @Override
    protected void cleanupEntityOnPartitionRemoval(EntityId entityId) {
        myUsageStates.remove(entityId);
    }

    private void persistAndNotify(BaseApiUsageState state, Map<ApiFeature, ApiUsageStateValue> result) {
        log.info("[{}] Detected update of the API state for {}: {}", state.getEntityId(), state.getEntityType(), result);
        ApiUsageState updatedState = apiUsageStateService.update(state.getApiUsageState());
        state.setApiUsageState(updatedState);
        long ts = System.currentTimeMillis();
        List<TsKvEntry> stateTelemetry = new ArrayList<>();
        result.forEach((apiFeature, aState) -> stateTelemetry.add(new BasicTsKvEntry(ts, new StringDataEntry(apiFeature.getApiStateKey(), aState.name()))));
        tsWsService.saveTimeseriesInternal(TimeseriesSaveRequest.builder()
                .tenantId(state.getTenantId())
                .entityId(state.getApiUsageState().getId())
                .entries(stateTelemetry)
                .build());

        if (state.getEntityType() == EntityType.TENANT && !state.getEntityId().equals(TenantId.SYS_TENANT_ID)) {
            String email = tenantService.findTenantById(state.getTenantId()).getEmail();
            result.forEach((apiFeature, stateValue) -> {
                ApiUsageRecordState recordState = createApiUsageRecordState((TenantApiUsageState) state, apiFeature, stateValue);
                if (recordState == null) {
                    return;
                }
                notificationRuleProcessor.process(ApiUsageLimitTrigger.builder()
                        .tenantId(state.getTenantId())
                        .state(recordState)
                        .status(stateValue)
                        .build());
                if (StringUtils.isNotEmpty(email)) {
                    mailExecutor.submit(() -> {
                        try {
                            mailService.sendApiFeatureStateEmail(apiFeature, stateValue, email, recordState);
                        } catch (ThingsboardException e) {
                            log.warn("[{}] Can't send update of the API state to tenant with provided email [{}]", state.getTenantId(), email, e);
                        }
                    });
                }
            });
        }
    }

    private ApiUsageRecordState createApiUsageRecordState(TenantApiUsageState state, ApiFeature apiFeature, ApiUsageStateValue stateValue) {
        StateChecker checker = getStateChecker(stateValue);
        for (ApiUsageRecordKey apiUsageRecordKey : ApiUsageRecordKey.getKeys(apiFeature)) {
            long threshold = state.getProfileThreshold(apiUsageRecordKey);
            long warnThreshold = state.getProfileWarnThreshold(apiUsageRecordKey);
            long value = state.get(apiUsageRecordKey);
            if (checker.check(threshold, warnThreshold, value)) {
                return new ApiUsageRecordState(apiFeature, apiUsageRecordKey, threshold, value);
            }
        }
        return null;
    }

    private StateChecker getStateChecker(ApiUsageStateValue stateValue) {
        if (ApiUsageStateValue.ENABLED.equals(stateValue)) {
            return (t, wt, v) -> true;
        } else if (ApiUsageStateValue.WARNING.equals(stateValue)) {
            return (t, wt, v) -> v < t && v >= wt;
        } else {
            return (t, wt, v) -> t > 0 && v >= t;
        }
    }

    @Override
    public ApiUsageState findApiUsageStateById(TenantId tenantId, ApiUsageStateId id) {
        return apiUsageStateService.findApiUsageStateById(tenantId, id);
    }

    private interface StateChecker {
        boolean check(long threshold, long warnThreshold, long value);
    }

    private void checkStartOfNextCycle() {
        updateLock.lock();
        try {
            long now = System.currentTimeMillis();
            myUsageStates.values().forEach(state -> {
                if ((state.getNextCycleTs() < now) && (now - state.getNextCycleTs() < TimeUnit.HOURS.toMillis(1))) {
                    state.setCycles(state.getNextCycleTs(), SchedulerUtils.getStartOfNextNextMonth());
                    if (log.isTraceEnabled()) {
                        log.trace("[{}][{}] Updating state cycles (currentCycleTs={},nextCycleTs={})", state.getTenantId(), state.getEntityId(), state.getCurrentCycleTs(), state.getNextCycleTs());
                    }
                    saveNewCounts(state, Arrays.asList(ApiUsageRecordKey.values()));
                    if (state.getEntityType() == EntityType.TENANT && !state.getEntityId().equals(TenantId.SYS_TENANT_ID)) {
                        TenantId tenantId = state.getTenantId();
                        updateTenantState((TenantApiUsageState) state, tenantProfileCache.get(tenantId));
                    }
                }
            });
        } catch (Throwable e) {
            log.error("Failed to check start of next cycle", e);
        } finally {
            updateLock.unlock();
        }
    }

    /**
     * 保存新的统计计数（重置为0）
     * @param state API使用状态
     * @param keys 要重置的API使用记录键列表
     */
    private void saveNewCounts(BaseApiUsageState state, List<ApiUsageRecordKey> keys) {
        List<TsKvEntry> counts = keys.stream()
                .map(key -> new BasicTsKvEntry(state.getCurrentCycleTs(), new LongDataEntry(key.getApiCountKey(), 0L)))
                .collect(Collectors.toList());

        tsWsService.saveTimeseriesInternal(TimeseriesSaveRequest.builder()
                .tenantId(state.getTenantId())
                .entityId(state.getApiUsageState().getId())
                .entries(counts)
                .build());
    }

    /**
     * 获取或创建对应实体的API使用统计对象
     * @param tenantId 租户ID
     * @param ownerId 所有者ID（租户ID或客户ID）
     * @return API使用状态
     */
    BaseApiUsageState getOrFetchState(TenantId tenantId, EntityId ownerId) {
        if (ownerId == null || ownerId.isNullUid()) {
            ownerId = tenantId;
        }
        // 首先从本地缓存查找
        BaseApiUsageState state = myUsageStates.get(ownerId);
        if (state != null) {
            return state;
        }

        // 从数据库加载状态
        ApiUsageState storedState = apiUsageStateService.findApiUsageStateByEntityId(ownerId);
        if (storedState == null) {
            try {
                // 创建默认的API使用状态
                storedState = apiUsageStateService.createDefaultApiUsageState(tenantId, ownerId);
            } catch (Exception e) {
                // 如果创建失败，再次尝试从数据库查找
                storedState = apiUsageStateService.findApiUsageStateByEntityId(ownerId);
            }
        }
        // 根据实体类型创建相应的状态对象
        if (ownerId.getEntityType() == EntityType.TENANT) {
            if (!ownerId.equals(TenantId.SYS_TENANT_ID)) {
                state = new TenantApiUsageState(tenantProfileCache.get((TenantId) ownerId), storedState);
            } else {
                // 系统租户
                state = new TenantApiUsageState(storedState);
            }
        } else {
            state = new CustomerApiUsageState(storedState);
        }

        List<ApiUsageRecordKey> newCounts = new ArrayList<>();
        try {
            // 从时间序列数据库加载最新的历史统计值，判断是否需要
            List<TsKvEntry> dbValues = tsService.findAllLatest(tenantId, storedState.getId()).get();
            for (ApiUsageRecordKey key : ApiUsageRecordKey.values()) {
                // 周期统计是否存在
                boolean cycleEntryFound = false;
                // 小时统计是否存在
                boolean hourlyEntryFound = false;
                for (TsKvEntry tsKvEntry : dbValues) {
                    // 查找周期统计值
                    if (tsKvEntry.getKey().equals(key.getApiCountKey())) {
                        cycleEntryFound = true;

                        // 检查是否为当前周期（也就是是否是本月或者本周）的统计值
                        boolean oldCount = tsKvEntry.getTs() == state.getCurrentCycleTs();
                        state.set(key, oldCount ? tsKvEntry.getLongValue().get() : 0L);

                        if (!oldCount) {
                            // 表示不是当前周期的数据，放入需要重置的键
                            newCounts.add(key);
                        }
                    } else if (tsKvEntry.getKey().equals(key.getApiCountKey() + HOURLY)) {
                        // 查找小时统计值
                        hourlyEntryFound = true;
                        state.setHourly(key, tsKvEntry.getTs() == state.getCurrentHourTs() ? tsKvEntry.getLongValue().get() : 0L);
                    }
                    if (cycleEntryFound && hourlyEntryFound) {
                        // 当一种api使用统计值都已经找到，则跳出循环
                        break;
                    }
                }
            }
            state.setGaugeReportInterval(gaugeReportInterval);
            log.debug("[{}][{}] Initialized state: {}", tenantId, ownerId, state);
            // 添加到本地缓存
            TopicPartitionInfo tpi = partitionService.resolve(ServiceType.TB_CORE, tenantId, ownerId);
            if (tpi.isMyPartition()) {
                addEntityState(tpi, state);
            } else {
                otherUsageStates.put(ownerId, state.getApiUsageState());
            }
            // 保存需要重置的统计值
            saveNewCounts(state, newCounts);
        } catch (InterruptedException | ExecutionException e) {
            log.warn("[{}] Failed to fetch api usage state from db.", tenantId, e);
        }

        return state;
    }

    @Override
    protected void onRepartitionEvent() {
        otherUsageStates.entrySet().removeIf(entry ->
                partitionService.resolve(ServiceType.TB_CORE, entry.getValue().getTenantId(), entry.getKey()).isMyPartition());
        updateLock.lock();
        try {
            myUsageStates.values().forEach(BaseApiUsageState::onRepartitionEvent);
        } finally {
            updateLock.unlock();
        }
    }

    @Override
    protected Map<TopicPartitionInfo, List<ListenableFuture<?>>> onAddedPartitions(Set<TopicPartitionInfo> addedPartitions) {
        var result = new HashMap<TopicPartitionInfo, List<ListenableFuture<?>>>();
        try {
            log.info("Initializing tenant states.");
            updateLock.lock();
            try {
                PageDataIterable<Tenant> tenantIterator = new PageDataIterable<>(tenantService::findTenants, 1024);
                for (Tenant tenant : tenantIterator) {
                    TopicPartitionInfo tpi = partitionService.resolve(ServiceType.TB_CORE, tenant.getId(), tenant.getId());
                    if (addedPartitions.contains(tpi)) {
                        if (!myUsageStates.containsKey(tenant.getId()) && tpi.isMyPartition()) {
                            log.debug("[{}] Initializing tenant state.", tenant.getId());
                            result.computeIfAbsent(tpi, tmp -> new ArrayList<>()).add(dbExecutor.submit(() -> {
                                try {
                                    updateTenantState((TenantApiUsageState) getOrFetchState(tenant.getId(), tenant.getId()), tenantProfileCache.get(tenant.getTenantProfileId()));
                                    log.debug("[{}] Initialized tenant state.", tenant.getId());
                                } catch (Exception e) {
                                    log.warn("[{}] Failed to initialize tenant API state", tenant.getId(), e);
                                }
                                return null;
                            }));
                        }
                    } else {
                        log.debug("[{}][{}] Tenant doesn't belong to current partition. tpi [{}]", tenant.getName(), tenant.getId(), tpi);
                    }
                }
            } finally {
                updateLock.unlock();
            }
        } catch (Exception e) {
            log.warn("Unknown failure", e);
        }
        return result;
    }

    @PreDestroy
    private void destroy() {
        super.stop();
    }
}
