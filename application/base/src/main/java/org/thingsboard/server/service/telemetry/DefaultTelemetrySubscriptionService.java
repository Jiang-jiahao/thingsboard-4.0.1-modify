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
package org.thingsboard.server.service.telemetry;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.DonAsynchron;
import org.thingsboard.common.util.ThingsBoardThreadFactory;
import org.thingsboard.rule.engine.api.AttributesDeleteRequest;
import org.thingsboard.rule.engine.api.AttributesSaveRequest;
import org.thingsboard.rule.engine.api.DeviceStateManager;
import org.thingsboard.rule.engine.api.RuleEngineTelemetryService;
import org.thingsboard.rule.engine.api.TimeseriesDeleteRequest;
import org.thingsboard.rule.engine.api.TimeseriesSaveRequest;
import org.thingsboard.server.common.data.ApiUsageRecordKey;
import org.thingsboard.server.common.data.AttributeScope;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.EntityView;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.data.kv.TimeseriesSaveResult;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.data.kv.TsKvLatestRemovingResult;
import org.thingsboard.server.common.msg.queue.TbCallback;
import org.thingsboard.server.common.msg.rule.engine.DeviceAttributesEventNotificationMsg;
import org.thingsboard.server.common.stats.TbApiUsageReportClient;
import org.thingsboard.server.dao.attributes.AttributesService;
import org.thingsboard.server.dao.timeseries.TimeseriesService;
import org.thingsboard.server.dao.util.KvUtils;
import org.thingsboard.server.service.apiusage.TbApiUsageStateService;
import org.thingsboard.server.service.cf.CalculatedFieldQueueService;
import org.thingsboard.server.service.entitiy.entityview.TbEntityViewService;
import org.thingsboard.server.service.state.constants.DefaultDeviceStateConstants;
import org.thingsboard.server.service.subscription.TbSubscriptionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingLong;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsFirst;

/**
 * 遥测数据（时序 + 属性）的写入与删除服务实现。
 * <p>
 * 同时实现 {@link RuleEngineTelemetryService}（供规则引擎节点调用）与 {@link TelemetrySubscriptionService}。
 * 每次写入/删除除了落库，还会按请求策略（Strategy）串起一组旁路逻辑：
 * <ul>
 *   <li>计算字段：策略开启时把请求推给 {@link CalculatedFieldQueueService} 异步计算；</li>
 *   <li>WebSocket 订阅：把变更推送给在线订阅者（仪表板实时刷新）；</li>
 *   <li>API 用量：时序数据点计数上报 {@link TbApiUsageReportClient}；</li>
 *   <li>设备联动：共享属性变更通知在线设备；server 属性 inactivityTimeout 变化同步给设备状态管理器；</li>
 *   <li>实体视图：设备/资产的最新值复制到关联的 {@link EntityView}。</li>
 * </ul>
 * 落库结果回调统一挂在 {@code tsCallBackExecutor} 单线程池上执行，保证回调串行有序。
 *
 * @see AbstractSubscriptionService
 */
@Service
@Slf4j
public class DefaultTelemetrySubscriptionService extends AbstractSubscriptionService implements TelemetrySubscriptionService, RuleEngineTelemetryService {

    /** 属性 KV 存储 */
    private final AttributesService attrService;
    /** 时序 KV 存储 */
    private final TimeseriesService tsService;
    /** 实体视图服务：设备/资产最新值复制目标 */
    private final Optional<TbEntityViewService> tbEntityViewService;
    /** API 用量上报客户端（数据点计数） */
    private final TbApiUsageReportClient apiUsageClient;
    /** API 用量状态：DB 写入是否被限额禁用 */
    private final TbApiUsageStateService apiUsageStateService;
    /** 计算字段异步队列 */
    private final CalculatedFieldQueueService calculatedFieldQueueService;
    /** 设备状态管理器：inactivityTimeout 联动 */
    private final Optional<DeviceStateManager> deviceStateManager;

    /** 落库结果回调专用单线程池，保证回调串行有序 */
    private ExecutorService tsCallBackExecutor;

    @Value("${sql.ts.value_no_xss_validation:false}")
    private boolean valueNoXssValidation;

    public DefaultTelemetrySubscriptionService(AttributesService attrService,
                                               TimeseriesService tsService,
                                               Optional<TbEntityViewService> tbEntityViewService,
                                               TbApiUsageReportClient apiUsageClient,
                                               TbApiUsageStateService apiUsageStateService,
                                               CalculatedFieldQueueService calculatedFieldQueueService,
                                               Optional<DeviceStateManager> deviceStateManager) {
        this.attrService = attrService;
        this.tsService = tsService;
        this.tbEntityViewService = tbEntityViewService;
        this.apiUsageClient = apiUsageClient;
        this.apiUsageStateService = apiUsageStateService;
        this.calculatedFieldQueueService = calculatedFieldQueueService;
        this.deviceStateManager = deviceStateManager;
    }

    @PostConstruct
    public void initExecutor() {
        super.initExecutor();
        // 遥测回调统一在单线程池执行，保证回调按提交顺序串行处理
        tsCallBackExecutor = Executors.newSingleThreadExecutor(ThingsBoardThreadFactory.forName("ts-service-ts-callback"));
    }

    @Override
    protected String getExecutorPrefix() {
        return "ts";
    }

    @PreDestroy
    public void shutdownExecutor() {
        if (tsCallBackExecutor != null) {
            tsCallBackExecutor.shutdownNow();
        }
        super.shutdownExecutor();
    }

    @Override
    public void saveTimeseries(TimeseriesSaveRequest request) {
        TenantId tenantId = request.getTenantId();
        EntityId entityId = request.getEntityId();
        // API_USAGE_STATE 实体不允许直接写，防止用量状态被业务数据绕过
        checkInternalEntity(entityId);
        boolean sysTenant = TenantId.SYS_TENANT_ID.equals(tenantId) || tenantId == null;
        // 系统租户 / 策略不要求存时序 / DB 存储未被 API 限额禁用 时才允许落库
        if (sysTenant || !request.getStrategy().saveTimeseries() || apiUsageStateService.getApiUsageState(tenantId).isDbStorageEnabled()) {
            KvUtils.validate(request.getEntries(), valueNoXssValidation);
            ListenableFuture<TimeseriesSaveResult> future = saveTimeseriesInternal(request);
            // 落库成功后把写入的数据点数量上报 API 用量
            if (request.getStrategy().saveTimeseries()) {
                Futures.addCallback(future, getApiUsageCallback(tenantId, request.getCustomerId(), sysTenant), tsCallBackExecutor);
            }
        } else {
            // API 限额已禁用 DB 存储：直接失败，不再写库
            request.getCallback().onFailure(new RuntimeException("DB storage writes are disabled due to API limits!"));
        }
    }

    @Override
    public ListenableFuture<TimeseriesSaveResult> saveTimeseriesInternal(TimeseriesSaveRequest request) {
        TenantId tenantId = request.getTenantId();
        EntityId entityId = request.getEntityId();
        TimeseriesSaveRequest.Strategy strategy = request.getStrategy();
        ListenableFuture<TimeseriesSaveResult> resultFuture;

        // 按策略组合选择落库方式：时序+最新 / 仅最新 / 仅时序 / 不落库
        if (strategy.saveTimeseries() && strategy.saveLatest()) {
            resultFuture = tsService.save(tenantId, entityId, request.getEntries(), request.getTtl());
        } else if (strategy.saveLatest()) {
            resultFuture = tsService.saveLatest(tenantId, entityId, request.getEntries());
        } else if (strategy.saveTimeseries()) {
            resultFuture = tsService.saveWithoutLatest(tenantId, entityId, request.getEntries(), request.getTtl());
        } else {
            resultFuture = Futures.immediateFuture(TimeseriesSaveResult.EMPTY);
        }

        // 主回调：策略要求计算字段时先推计算队列，由计算结果再回调原回调；否则直接回调成功
        addMainCallback(resultFuture, result -> {
            if (strategy.processCalculatedFields()) {
                calculatedFieldQueueService.pushRequestToQueue(request, result, request.getCallback());
            } else {
                request.getCallback().onSuccess(null);
            }
        }, t -> request.getCallback().onFailure(t));

        // WebSocket 订阅者实时推送
        if (strategy.sendWsUpdate()) {
            addWsCallback(resultFuture, success -> onTimeSeriesUpdate(tenantId, entityId, request.getEntries()));
        }
        // 设备/资产的最新值同步复制到关联实体视图
        if (strategy.saveLatest() && entityId.getEntityType().isOneOf(EntityType.DEVICE, EntityType.ASSET)) {
            addMainCallback(resultFuture, __ -> copyLatestToEntityViews(tenantId, entityId, request.getEntries()));
        }
        return resultFuture;
    }

    @Override
    public void saveAttributes(AttributesSaveRequest request) {
        checkInternalEntity(request.getEntityId());
        saveAttributesInternal(request);
    }

    @Override
    public void saveAttributesInternal(AttributesSaveRequest request) {
        log.trace("Executing saveInternal [{}]", request);
        TenantId tenantId = request.getTenantId();
        EntityId entityId = request.getEntityId();
        AttributesSaveRequest.Strategy strategy = request.getStrategy();
        ListenableFuture<List<Long>> resultFuture;

        // 按策略决定是否真正落库（不落库时立即返回空结果）
        if (strategy.saveAttributes()) {
            resultFuture = attrService.save(tenantId, entityId, request.getScope(), request.getEntries());
        } else {
            resultFuture = Futures.immediateFuture(Collections.emptyList());
        }

        // 主回调：策略要求计算字段时先推计算队列，否则直接回调成功
        addMainCallback(resultFuture, result -> {
            if (strategy.processCalculatedFields()) {
                calculatedFieldQueueService.pushRequestToQueue(request, result, request.getCallback());
            } else {
                request.getCallback().onSuccess(null);
            }
        }, t -> request.getCallback().onFailure(t));

        // 共享属性更新：通知在线设备（如通过 MQTT 下发新配置）
        if (shouldSendSharedAttributesUpdatedNotification(request)) {
            addMainCallback(resultFuture, success -> clusterService.pushMsgToCore(
                    DeviceAttributesEventNotificationMsg.onUpdate(tenantId, new DeviceId(entityId.getId()), DataConstants.SHARED_SCOPE, request.getEntries()), null
            ));
        }

        // server 属性里的 inactivityTimeout 变化：同步给设备状态管理器，用于设备不活跃判定
        if (shouldCheckForInactivityTimeoutUpdates(request)) {
            deviceStateManager.ifPresent(dsm ->
                    findNewInactivityTimeout(request.getEntries()).ifPresent(newInactivityTimeout ->
                            addMainCallback(resultFuture, success -> dsm.onDeviceInactivityTimeoutUpdate(
                                    tenantId, new DeviceId(entityId.getId()), newInactivityTimeout, TbCallback.EMPTY)
                            )
                    )
            );
        }

        // WebSocket 订阅者推送
        if (strategy.sendWsUpdate()) {
            addWsCallback(resultFuture, success -> onAttributesUpdate(tenantId, entityId, request.getScope().name(), request.getEntries()));
        }
    }

    /** 保存共享属性且请求方要求通知设备时才下发通知 */
    private static boolean shouldSendSharedAttributesUpdatedNotification(AttributesSaveRequest request) {
        return request.getStrategy().saveAttributes() && shouldSendSharedAttributesNotification(request.getEntityId(), request.getScope(), request.isNotifyDevice());
    }

    /** 只有设备实体的 server 属性才需要检查 inactivityTimeout */
    private static boolean shouldCheckForInactivityTimeoutUpdates(AttributesSaveRequest request) {
        return request.getStrategy().saveAttributes()
                && request.getEntityId().getEntityType() == EntityType.DEVICE
                && request.getScope() == AttributeScope.SERVER_SCOPE;
    }

    /**
     * 从本次写入的属性里找出新的 inactivityTimeout 值。
     * 取版本号最高的条目；版本相同则取最近更新时间最晚的，再解析为 long（解析失败按 0 处理）。
     */
    private static Optional<Long> findNewInactivityTimeout(List<AttributeKvEntry> entries) {
        return entries.stream()
                .filter(entry -> Objects.equals(DefaultDeviceStateConstants.INACTIVITY_TIMEOUT, entry.getKey()))
                .max(comparing(AttributeKvEntry::getVersion, nullsFirst(naturalOrder())).thenComparingLong(AttributeKvEntry::getLastUpdateTs))
                .map(DefaultTelemetrySubscriptionService::parseAsLong);
    }

    private static long parseAsLong(KvEntry kve) {
        try {
            return Long.parseLong(kve.getValueAsString());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    @Override
    public void deleteAttributes(AttributesDeleteRequest request) {
        checkInternalEntity(request.getEntityId());
        deleteAttributesInternal(request);
    }

    @Override
    public void deleteAttributesInternal(AttributesDeleteRequest request) {
        TenantId tenantId = request.getTenantId();
        EntityId entityId = request.getEntityId();

        ListenableFuture<List<String>> deleteFuture = attrService.removeAll(tenantId, entityId, request.getScope(), request.getKeys());

        // 删除后同步触发计算字段重算（被删属性可能参与了某个计算字段）
        addMainCallback(deleteFuture,
                result -> calculatedFieldQueueService.pushRequestToQueue(request, result, request.getCallback()),
                t -> request.getCallback().onFailure(t)
        );

        // 共享属性删除：通知在线设备
        if (shouldSendSharedAttributesDeletedNotification(request)) {
            addMainCallback(deleteFuture, success -> clusterService.pushMsgToCore(
                    DeviceAttributesEventNotificationMsg.onDelete(tenantId, new DeviceId(entityId.getId()), DataConstants.SHARED_SCOPE, request.getKeys()), null
            ));
        }

        // 删除的是 inactivityTimeout：重置为 0，表示设备不再按不活跃超时判定
        if (inactivityTimeoutDeleted(request)) {
            deviceStateManager.ifPresent(dsm ->
                    addMainCallback(deleteFuture, success -> dsm.onDeviceInactivityTimeoutUpdate(
                            tenantId, new DeviceId(entityId.getId()), 0L, TbCallback.EMPTY)
                    )
            );
        }

        // WebSocket 订阅者推送
        addWsCallback(deleteFuture, success -> onAttributesDelete(tenantId, entityId, request.getScope().name(), request.getKeys()));
    }

    private static boolean shouldSendSharedAttributesDeletedNotification(AttributesDeleteRequest request) {
        return shouldSendSharedAttributesNotification(request.getEntityId(), request.getScope(), request.isNotifyDevice());
    }

    /** 只有设备 + SHARED_SCOPE + 请求方要求通知设备 时才下发共享属性通知 */
    private static boolean shouldSendSharedAttributesNotification(EntityId entityId, AttributeScope scope, boolean notifyDevice) {
        return entityId.getEntityType() == EntityType.DEVICE
                && scope == AttributeScope.SHARED_SCOPE
                && notifyDevice;
    }

    /** 删除的是设备 server 属性里的 inactivityTimeout 时返回 true */
    private static boolean inactivityTimeoutDeleted(AttributesDeleteRequest request) {
        return request.getEntityId().getEntityType() == EntityType.DEVICE
                && request.getScope() == AttributeScope.SERVER_SCOPE
                && request.getKeys().stream().anyMatch(key -> Objects.equals(DefaultDeviceStateConstants.INACTIVITY_TIMEOUT, key));
    }

    @Override
    public void deleteTimeseries(TimeseriesDeleteRequest request) {
        checkInternalEntity(request.getEntityId());
        deleteTimeseriesInternal(request);
    }

    @Override
    public void deleteTimeseriesInternal(TimeseriesDeleteRequest request) {
        if (CollectionUtils.isNotEmpty(request.getKeys())) {
            ListenableFuture<List<TsKvLatestRemovingResult>> deleteFuture;
            if (request.getDeleteHistoryQueries() == null) {
                // 只删最新值（保留历史数据）
                deleteFuture = tsService.removeLatest(request.getTenantId(), request.getEntityId(), request.getKeys());
            } else {
                // 按查询条件删历史区间，并把删除对最新值的影响推给 WebSocket 订阅者
                deleteFuture = tsService.remove(request.getTenantId(), request.getEntityId(), request.getDeleteHistoryQueries());
                addWsCallback(deleteFuture, result -> onTimeSeriesDelete(request.getTenantId(), request.getEntityId(), request.getKeys(), result));
            }
            // 删除后触发计算字段重算，完成后把 key 列表回传原回调
            DonAsynchron.withCallback(deleteFuture, result -> {
                calculatedFieldQueueService.pushRequestToQueue(request, request.getKeys(), getCalculatedFieldCallback(request.getCallback(), request.getKeys()));
            }, safeCallback(getCalculatedFieldCallback(request.getCallback(), request.getKeys())), tsCallBackExecutor);
        } else {
            // 未指定 keys：删除该实体全部最新值
            ListenableFuture<List<String>> deleteFuture = tsService.removeAllLatest(request.getTenantId(), request.getEntityId());
            DonAsynchron.withCallback(deleteFuture, result -> {
                calculatedFieldQueueService.pushRequestToQueue(request, request.getKeys(), getCalculatedFieldCallback(request.getCallback(), result));
            }, safeCallback(getCalculatedFieldCallback(request.getCallback(), request.getKeys())), tsCallBackExecutor);
        }
    }

    /**
     * 把设备/资产刚写入的时序最新值复制到所有关联的实体视图。
     * 只复制实体视图 keys 里声明的时序键、且时间戳落在 (startTimeMs, endTimeMs] 窗口内的点，
     * 每个键取窗口内最新的一个点，以 LATEST_AND_WS 策略写回视图实体。
     */
    private void copyLatestToEntityViews(TenantId tenantId, EntityId entityId, List<TsKvEntry> ts) {
        if (tbEntityViewService.isEmpty()) {
            return;
        }
        Futures.addCallback(tbEntityViewService.get().findEntityViewsByTenantIdAndEntityIdAsync(tenantId, entityId),
                new FutureCallback<>() {
                    @Override
                    public void onSuccess(@Nullable List<EntityView> result) {
                        if (result != null && !result.isEmpty()) {
                            // 按 key 分组，便于逐视图逐键查找
                            Map<String, List<TsKvEntry>> tsMap = new HashMap<>();
                            for (TsKvEntry entry : ts) {
                                tsMap.computeIfAbsent(entry.getKey(), s -> new ArrayList<>()).add(entry);
                            }
                            for (EntityView entityView : result) {
                                // 视图未声明时序 keys 时，复制全部 key
                                List<String> keys = entityView.getKeys() != null && entityView.getKeys().getTimeseries() != null ?
                                        entityView.getKeys().getTimeseries() : new ArrayList<>(tsMap.keySet());
                                List<TsKvEntry> entityViewLatest = new ArrayList<>();
                                long startTs = entityView.getStartTimeMs();
                                long endTs = entityView.getEndTimeMs() == 0 ? Long.MAX_VALUE : entityView.getEndTimeMs();
                                for (String key : keys) {
                                    List<TsKvEntry> entries = tsMap.get(key);
                                    if (entries != null) {
                                        // 只取 (startTs, endTs] 窗口内时间戳最大的一个点
                                        Optional<TsKvEntry> tsKvEntry = entries.stream()
                                                .filter(entry -> entry.getTs() > startTs && entry.getTs() <= endTs)
                                                .max(comparingLong(TsKvEntry::getTs));
                                        tsKvEntry.ifPresent(entityViewLatest::add);
                                    }
                                }
                                if (!entityViewLatest.isEmpty()) {
                                    saveTimeseries(TimeseriesSaveRequest.builder()
                                            .tenantId(tenantId)
                                            .entityId(entityView.getId())
                                            .entries(entityViewLatest)
                                            .strategy(TimeseriesSaveRequest.Strategy.LATEST_AND_WS)
                                            .callback(new FutureCallback<>() {
                                                @Override
                                                public void onSuccess(@Nullable Void tmp) {}

                                                @Override
                                                public void onFailure(Throwable t) {
                                                    log.error("[{}][{}] Failed to save entity view latest timeseries: {}", tenantId, entityView.getId(), entityViewLatest, t);
                                                }
                                            })
                                            .build());
                                }
                            }
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        log.error("Error while finding entity views by tenantId and entityId", t);
                    }
                }, MoreExecutors.directExecutor());
    }

    /** 属性更新 → 推送给在线 WebSocket 订阅者（本地直推或跨节点 proto 转发） */
    private void onAttributesUpdate(TenantId tenantId, EntityId entityId, String scope, List<AttributeKvEntry> attributes) {
        forwardToSubscriptionManagerService(tenantId, entityId,
                subscriptionManagerService -> subscriptionManagerService.onAttributesUpdate(tenantId, entityId, scope, attributes, TbCallback.EMPTY),
                () -> TbSubscriptionUtils.toAttributesUpdateProto(tenantId, entityId, scope, attributes));
    }

    /** 属性删除 → 推送给在线 WebSocket 订阅者 */
    private void onAttributesDelete(TenantId tenantId, EntityId entityId, String scope, List<String> keys) {
        forwardToSubscriptionManagerService(tenantId, entityId,
                subscriptionManagerService -> subscriptionManagerService.onAttributesDelete(tenantId, entityId, scope, keys, TbCallback.EMPTY),
                () -> TbSubscriptionUtils.toAttributesDeleteProto(tenantId, entityId, scope, keys));
    }

    /** 时序更新 → 推送给在线 WebSocket 订阅者 */
    private void onTimeSeriesUpdate(TenantId tenantId, EntityId entityId, List<TsKvEntry> ts) {
        forwardToSubscriptionManagerService(tenantId, entityId,
                subscriptionManagerService -> subscriptionManagerService.onTimeSeriesUpdate(tenantId, entityId, ts, TbCallback.EMPTY),
                () -> TbSubscriptionUtils.toTimeseriesUpdateProto(tenantId, entityId, ts));
    }

    /**
     * 时序区间删除 → 推送给在线 WebSocket 订阅者。
     * 删除结果里区分：最新值被删但仍有历史数据（发更新）与最新值完全删除（发删除）。
     */
    private void onTimeSeriesDelete(TenantId tenantId, EntityId entityId, List<String> keys, List<TsKvLatestRemovingResult> ts) {
        forwardToSubscriptionManagerService(tenantId, entityId, subscriptionManagerService -> {
            List<TsKvEntry> updated = new ArrayList<>();
            List<String> deleted = new ArrayList<>();

            ts.stream().filter(Objects::nonNull).forEach(res -> {
                if (res.isRemoved()) {
                    if (res.getData() != null) {
                        updated.add(res.getData());
                    } else {
                        deleted.add(res.getKey());
                    }
                }
            });

            subscriptionManagerService.onTimeSeriesUpdate(tenantId, entityId, updated, TbCallback.EMPTY);
            subscriptionManagerService.onTimeSeriesDelete(tenantId, entityId, deleted, TbCallback.EMPTY);
        }, () -> TbSubscriptionUtils.toTimeseriesDeleteProto(tenantId, entityId, keys));
    }

    private <S> void addMainCallback(ListenableFuture<S> saveFuture, final FutureCallback<Void> callback) {
        if (callback == null) return;
        addMainCallback(saveFuture, result -> callback.onSuccess(null), callback::onFailure);
    }

    private <S> void addMainCallback(ListenableFuture<S> saveFuture, Consumer<S> onSuccess) {
        addMainCallback(saveFuture, onSuccess, null);
    }

    /** 把落库结果回调统一挂到 tsCallBackExecutor 单线程池：回调串行执行，避免并发问题 */
    private <S> void addMainCallback(ListenableFuture<S> saveFuture, Consumer<S> onSuccess, Consumer<Throwable> onFailure) {
        DonAsynchron.withCallback(saveFuture, onSuccess, onFailure, tsCallBackExecutor);
    }

    /** API_USAGE_STATE 是内部维护的用量状态实体，禁止业务直接写遥测 */
    private void checkInternalEntity(EntityId entityId) {
        if (EntityType.API_USAGE_STATE.equals(entityId.getEntityType())) {
            throw new RuntimeException("Can't update API Usage State!");
        }
    }

    /** 时序落库成功后，把写入的数据点数量上报到 API 用量统计（系统租户不统计） */
    private FutureCallback<TimeseriesSaveResult> getApiUsageCallback(TenantId tenantId, CustomerId customerId, boolean sysTenant) {
        return new FutureCallback<>() {
            @Override
            public void onSuccess(TimeseriesSaveResult result) {
                Integer dataPoints = result.getDataPoints();
                if (!sysTenant && dataPoints != null && dataPoints > 0) {
                    apiUsageClient.report(tenantId, customerId, ApiUsageRecordKey.STORAGE_DP_COUNT, dataPoints);
                }
            }

            @Override
            public void onFailure(Throwable t) {}
        };
    }

    /** 包装原回调：计算字段处理完成后，把受影响的 key 列表回传 */
    private FutureCallback<Void> getCalculatedFieldCallback(FutureCallback<List<String>> originalCallback, List<String> keys) {
        return new FutureCallback<>() {
            @Override
            public void onSuccess(Void unused) {
                originalCallback.onSuccess(keys);
            }

            @Override
            public void onFailure(Throwable t) {
                originalCallback.onFailure(t);
            }
        };
    }

}
