/**
 * Copyright 婕� 2016-2025 The Thingsboard Authors
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
import java.util.Optional;
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
    private final Optional<NotificationRuleProcessor> notificationRuleProcessor;
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
     * core閺堝秴濮熼幒銉︽暪楠炶泛顦╅悶鍡樻降閼奉亜鍙炬禒鏍т簳閺堝秴濮熼敍鍫濐洤鐟欏嫬鍨鏇熸惛閵嗕椒绱舵潏鎾崇湴閿涘绗傞幎銉ф畱API娴ｈ法鏁ら弫鐗堝祦
     * @param msgPack api娴ｈ法鏁ら弫鐗堝祦
     * @param callback 閸ョ偠鐨熼崙鑺ユ殶
     */
    @Override
    public void process(TbProtoQueueMsg<ToUsageStatsServiceMsg> msgPack, TbCallback callback) {
        ToUsageStatsServiceMsg serviceMsg = msgPack.getValue();
        String serviceId = serviceMsg.getServiceId();

        List<TransportProtos.UsageStatsServiceMsg> msgs;

        //For backward compatibility, remove after release
        if (serviceMsg.getMsgsList().isEmpty()) {
            // 閺傛壆澧楅張顒傛畱濞戝牊浼呮担鎿勭礉閺嬪嫬缂撻弮褏娈戝☉鍫熶紖娴ｏ拷
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
            // 鐞涖劎銇氶弰顖涙＋閻楀牊婀伴惃鍕Х閹拷
            msgs = serviceMsg.getMsgsList();
        }

        msgs.forEach(msg -> {
            TenantId tenantId = TenantId.fromUUID(new UUID(msg.getTenantIdMSB(), msg.getTenantIdLSB()));
            EntityId ownerId;
            // 閸掋倖鏌囬弰顖氼吂閹寸柉绻曢弰顖滎潳閹达拷
            if (msg.getCustomerIdMSB() != 0 && msg.getCustomerIdLSB() != 0) {
                ownerId = new CustomerId(new UUID(msg.getCustomerIdMSB(), msg.getCustomerIdLSB()));
            } else {
                ownerId = tenantId;
            }
            // 婢跺嫮鎮婄€圭偘缍嬮惃鍑橮I娴ｈ法鏁ょ紒鐔活吀閺佺増宓�
            processEntityUsageStats(tenantId, ownerId, msg.getValuesList(), serviceId);
        });
        callback.onSuccess();
    }

    /**
     * 1閵嗕浇骞忛崣鏍ь嚠鎼存梻娈戠€圭偘缍媋pi娴ｈ法鏁ょ紒鐔活吀鐎电钖勯獮鑸典划婢跺秴顕惔鏃€鏆熼幑顕嗙礄閹垹顦查惃鍕埠鐠佲剝鏆熼幑顔芥降濠ф劒绨弫鐗堝祦鎼存挸鐡ㄩ崒銊ф畱閺佺増宓侀敍锟�
     * 2閵嗕礁鍨介弬顓犵埠鐠佲€愁嚠鐠炩剝鏆熼幑顔芥Ц閸氾箑褰傞悽鐔告暭閸欐﹫绱濇俊鍌涚亯閸欐垹鏁撻弨鐟板綁閿涘苯鍨弴瀛樻煀閺佺増宓佹惔锟�
     * @param tenantId 缁夌喐鍩汭D
     * @param ownerId 閹碘偓閺堝鈧將D閿涘牏顫ら幋绋〥閹存牕顓归幋绋〥閿涳拷
     * @param values API娴ｈ法鏁ょ紒鐔活吀闁款喖鈧厧顕崚妤勩€�
     * @param serviceId 娑撳﹥濮ら張宥呭鐎圭偘绶D
     */
    private void processEntityUsageStats(TenantId tenantId, EntityId ownerId, List<UsageStatsKVProto> values, String serviceId) {
        // 鐠哄疇绻冨鎻掑灩闂勩倗娈戠€圭偘缍�
        if (deletedEntities.contains(ownerId)) return;

        BaseApiUsageState usageState;
        // 闂団偓鐟曚焦娲块弬鎵畱閺冭泛绨弫鐗堝祦
        List<TsKvEntry> updatedEntries;
        Map<ApiFeature, ApiUsageStateValue> result;
        // 閸旂娀鏀ｇ涵顔荤箽閻樿埖鈧焦娲块弬鎵畱缁捐法鈻肩€瑰鍙�
        updateLock.lock();
        try {
            // 閼惧嘲褰囬幋鏍у灡瀵ゅ搫顕惔鏂跨杽娴ｆ挾娈慉PI娴ｈ法鏁ょ紒鐔活吀鐎电钖�
            usageState = getOrFetchState(tenantId, ownerId);
            long ts = usageState.getCurrentCycleTs();
            long hourTs = usageState.getCurrentHourTs();
            long newHourTs = SchedulerUtils.getStartOfCurrentHour();
            if (newHourTs != hourTs) {
                // 婵″倹鐏夐悳鏉挎躬閻ㄥ嫬鐨弮璺烘嫲閸樼喐娼甸惃鍕瑝閺勵垰鎮撴稉鈧稉顏勭毈閺冩湹绨￠敍宀勫亝娑斿牏娲块幒銉︽纯閺傛澘鐨弮锟�
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

                // 閺嶈宓侀崗鏈电铂閺堝秴濮熺€圭偘绶ョ紒娆忕暰閻ㄥ埍v閿涘本娼电拋锛勭暬閺傛壆娈戠紒鐔活吀閸婏拷
                StatsCalculationResult calculationResult = usageState.calculate(recordKey, statsItem.getValue(), serviceId);
                if (calculationResult.isValueChanged()) {
                    // 婵″倹鐏夐崨銊︽埂閹勬殶閹诡喖褰傞悽鐔告暭閸欐ü绨￠敍灞藉灟濞ｈ濮為崚浼存付鐟曚焦娲块弬鎵畱閺冭泛绨弫鐗堝祦娑擄拷
                    long newValue = calculationResult.getNewValue();
                    updatedEntries.add(new BasicTsKvEntry(ts, new LongDataEntry(recordKey.getApiCountKey(), newValue)));
                }
                if (calculationResult.isHourlyValueChanged()) {
                    // 婵″倹鐏夌亸蹇旀閺佺増宓侀崣鎴犳晸閺€鐟板綁娴滃棴绱濋崚娆愬潑閸旂姴鍩岄棁鈧憰浣规纯閺傛壆娈戦弮璺虹碍閺佺増宓佹稉锟�
                    long newHourlyValue = calculationResult.getNewHourlyValue();
                    updatedEntries.add(new BasicTsKvEntry(newHourTs, new LongDataEntry(recordKey.getApiCountKey() + HOURLY, newHourlyValue)));
                }
                if (recordKey.getApiFeature() != null) {
                    apiFeatures.add(recordKey.getApiFeature());
                }
            }
            // 濡偓閺屻儳顫ら幋椋庡Ц閹焦妲搁崥锕傛付鐟曚焦娲块弬甯礄鏉堟儳鍩岄梼鍫濃偓纭风礆
            // 濞夈劍鍓伴敍姘遍兇缂佺喓顫ら幋鍑ょ礄SYS_TENANT_ID閿涘绗夐崑姘舵閸掞拷
            if (usageState.getEntityType() == EntityType.TENANT && !usageState.getEntityId().equals(TenantId.SYS_TENANT_ID)) {
                result = ((TenantApiUsageState) usageState).checkStateUpdatedDueToThreshold(apiFeatures);
            } else {
                result = Collections.emptyMap();
            }
        } finally {
            updateLock.unlock();
        }
        log.trace("[{}][{}] Saving new stats: {}", tenantId, ownerId, updatedEntries);
        // 閺囧瓨鏌婇弫鐗堝祦鎼存挷鑵戦惃鍕鎼村繑鏆熼幑锟�
        tsWsService.saveTimeseriesInternal(TimeseriesSaveRequest.builder()
                .tenantId(tenantId)
                .entityId(usageState.getApiUsageState().getId())
                .entries(updatedEntries)
                .build());
        // 婵″倹鐏夐張澶屽Ц閹礁褰夐弴杈剧礄婵″倽鎻崚鎷岊劅閸涘﹥鍨ㄩ梽鎰煑闂冨牆鈧》绱氶敍灞藉絺闁線鈧氨鐓�
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
        // 瑜版彸pi娴ｈ法鏁ょ紒鐔活吀鐞氼偅娲块弬鎵畱閺冭泛鈧瑱绱濇禒搴ｇ处鐎涙ü鑵戦崚鐘绘珟鐠囥儳顫ら幋椋庢畱api娴ｈ法鏁ょ紒鐔活吀
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
     * 鐏忓棗鐤勬担鎾跺Ц閹焦鍧婇崝鐘插煂閺堫剚婀囬崝鈥虫珤閻ㄥ嫮顓搁悶鍡涙肠閸氬牅鑵�
     * @param tpi 娑撳顣介崚鍡楀隘娣団剝浼�
     * @param state API娴ｈ法鏁ら悩鑸碘偓锟�
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
                notificationRuleProcessor.ifPresent(p -> p.process(ApiUsageLimitTrigger.builder()
                        .tenantId(state.getTenantId())
                        .state(recordState)
                        .status(stateValue)
                        .build()));
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
     * 娣囨繂鐡ㄩ弬鎵畱缂佺喕顓哥拋鈩冩殶閿涘牓鍣哥純顔昏礋0閿涳拷
     * @param state API娴ｈ法鏁ら悩鑸碘偓锟�
     * @param keys 鐟曚線鍣哥純顔炬畱API娴ｈ法鏁ょ拋鏉跨秿闁款喖鍨悰锟�
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
     * 閼惧嘲褰囬幋鏍у灡瀵ゅ搫顕惔鏂跨杽娴ｆ挾娈慉PI娴ｈ法鏁ょ紒鐔活吀鐎电钖�
     * @param tenantId 缁夌喐鍩汭D
     * @param ownerId 閹碘偓閺堝鈧將D閿涘牏顫ら幋绋〥閹存牕顓归幋绋〥閿涳拷
     * @return API娴ｈ法鏁ら悩鑸碘偓锟�
     */
    BaseApiUsageState getOrFetchState(TenantId tenantId, EntityId ownerId) {
        if (ownerId == null || ownerId.isNullUid()) {
            ownerId = tenantId;
        }
        // 妫ｆ牕鍘涙禒搴㈡拱閸︽壆绱︾€涙ɑ鐓￠幍锟�
        BaseApiUsageState state = myUsageStates.get(ownerId);
        if (state != null) {
            return state;
        }

        // 娴犲孩鏆熼幑顔肩氨閸旂姾娴囬悩鑸碘偓锟�
        ApiUsageState storedState = apiUsageStateService.findApiUsageStateByEntityId(ownerId);
        if (storedState == null) {
            try {
                // 閸掓稑缂撴妯款吇閻ㄥ嚈PI娴ｈ法鏁ら悩鑸碘偓锟�
                storedState = apiUsageStateService.createDefaultApiUsageState(tenantId, ownerId);
            } catch (Exception e) {
                // 婵″倹鐏夐崚娑樼紦婢惰精瑙﹂敍灞藉晙濞嗏€崇毦鐠囨洑绮犻弫鐗堝祦鎼存挻鐓￠幍锟�
                storedState = apiUsageStateService.findApiUsageStateByEntityId(ownerId);
            }
        }
        // 閺嶈宓佺€圭偘缍嬬猾璇茬€烽崚娑樼紦閻╃ǹ绨查惃鍕Ц閹礁顕挒锟�
        if (ownerId.getEntityType() == EntityType.TENANT) {
            if (!ownerId.equals(TenantId.SYS_TENANT_ID)) {
                state = new TenantApiUsageState(tenantProfileCache.get((TenantId) ownerId), storedState);
            } else {
                // 缁崵绮虹粔鐔稿煕
                state = new TenantApiUsageState(storedState);
            }
        } else {
            state = new CustomerApiUsageState(storedState);
        }

        List<ApiUsageRecordKey> newCounts = new ArrayList<>();
        try {
            // 娴犲孩妞傞梻鏉戠碍閸掓鏆熼幑顔肩氨閸旂姾娴囬張鈧弬鎵畱閸樺棗褰剁紒鐔活吀閸婄》绱濋崚銈嗘焽閺勵垰鎯侀棁鈧憰锟�
            List<TsKvEntry> dbValues = tsService.findAllLatest(tenantId, storedState.getId()).get();
            for (ApiUsageRecordKey key : ApiUsageRecordKey.values()) {
                // 閸涖劍婀＄紒鐔活吀閺勵垰鎯佺€涙ê婀�
                boolean cycleEntryFound = false;
                // 鐏忓繑妞傜紒鐔活吀閺勵垰鎯佺€涙ê婀�
                boolean hourlyEntryFound = false;
                for (TsKvEntry tsKvEntry : dbValues) {
                    // 閺屻儲澹橀崨銊︽埂缂佺喕顓搁崐锟�
                    if (tsKvEntry.getKey().equals(key.getApiCountKey())) {
                        cycleEntryFound = true;

                        // 濡偓閺屻儲妲搁崥锔胯礋瑜版挸澧犻崨銊︽埂閿涘牅绡冪亸杈ㄦЦ閺勵垰鎯侀弰顖涙拱閺堝牊鍨ㄩ懓鍛拱閸涱煉绱氶惃鍕埠鐠佲€斥偓锟�
                        boolean oldCount = tsKvEntry.getTs() == state.getCurrentCycleTs();
                        state.set(key, oldCount ? tsKvEntry.getLongValue().get() : 0L);

                        if (!oldCount) {
                            // 鐞涖劎銇氭稉宥嗘Ц瑜版挸澧犻崨銊︽埂閻ㄥ嫭鏆熼幑顕嗙礉閺€鎯у弳闂団偓鐟曚線鍣哥純顔炬畱闁匡拷
                            newCounts.add(key);
                        }
                    } else if (tsKvEntry.getKey().equals(key.getApiCountKey() + HOURLY)) {
                        // 閺屻儲澹樼亸蹇旀缂佺喕顓搁崐锟�
                        hourlyEntryFound = true;
                        state.setHourly(key, tsKvEntry.getTs() == state.getCurrentHourTs() ? tsKvEntry.getLongValue().get() : 0L);
                    }
                    if (cycleEntryFound && hourlyEntryFound) {
                        // 瑜版挷绔寸粔宄歱i娴ｈ法鏁ょ紒鐔活吀閸婂ジ鍏樺鑼病閹垫儳鍩岄敍灞藉灟鐠哄啿鍤顏嗗箚
                        break;
                    }
                }
            }
            state.setGaugeReportInterval(gaugeReportInterval);
            log.debug("[{}][{}] Initialized state: {}", tenantId, ownerId, state);
            // 濞ｈ濮為崚鐗堟拱閸︽壆绱︾€涳拷
            TopicPartitionInfo tpi = partitionService.resolve(ServiceType.TB_CORE, tenantId, ownerId);
            if (tpi.isMyPartition()) {
                addEntityState(tpi, state);
            } else {
                otherUsageStates.put(ownerId, state.getApiUsageState());
            }
            // 娣囨繂鐡ㄩ棁鈧憰渚€鍣哥純顔炬畱缂佺喕顓搁崐锟�
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
