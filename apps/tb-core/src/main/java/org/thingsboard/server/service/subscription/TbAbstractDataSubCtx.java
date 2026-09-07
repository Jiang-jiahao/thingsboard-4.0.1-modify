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
package org.thingsboard.server.service.subscription;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.kv.Aggregation;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.query.*;
import org.thingsboard.server.dao.attributes.AttributesService;
import org.thingsboard.server.dao.entity.EntityService;
import org.thingsboard.server.service.ws.WebSocketService;
import org.thingsboard.server.service.ws.WebSocketSessionRef;
import org.thingsboard.server.service.ws.telemetry.sub.TelemetrySubscriptionUpdate;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 「有实体行」的查询 ctx：本页 {@link EntityData} + 按行拆到柜子里的内部订阅。
 *
 * <p>前端仍是一个 {@code cmdId}；柜子里是每个实体 × 每种 key 类型一条 {@code TbSubscription}。
 * {@link #subToEntityIdMap} 记住内部 subscriptionId 对应哪一行，增量回调时才能拼回这条实体。
 *
 * <h2>和上一层的分工</h2>
 * {@link TbAbstractEntityQuerySubCtx} 管 query、动态过滤值、刷新任务。
 * 本类管：{@link #fetchData} 拉本页、{@link #createLatestValuesSubscriptions} /
 * {@link #createTimeSeriesSubscriptions} 往柜子塞订阅、{@link #update} 对比新旧实体集合。
 *
 * <p>实体集合变了只调用 {@link #doUpdate}：子类决定给新行补订、给消失的行退订，并推整页更新。
 *
 * @param <T> 带 {@link EntityDataPageLink} 的数据 query（实体表、告警表等）
 * @see TbEntityDataSubCtx
 * @see TbAlarmDataSubCtx
 */
@Slf4j
public abstract class TbAbstractDataSubCtx<T extends AbstractDataQuery<? extends EntityDataPageLink>> extends TbAbstractEntityQuerySubCtx<T> {

    /**
     * 内部订阅 id → 实体。增量从柜子回来时只有 subscriptionId，靠这张表找回是哪一行。
     * 动态刷新丢掉某实体时，也靠它找出要 cancel 的订阅。
     */
    protected final Map<Integer, EntityId> subToEntityIdMap;

    /** 最近一次 query 的本页数据（含 latest / 刚查的 timeseries）。 */
    @Getter
    protected PageData<EntityData> data;

    public TbAbstractDataSubCtx(String serviceId, WebSocketService wsService,
                                EntityService entityService, TbLocalSubscriptionService localSubscriptionService,
                                AttributesService attributesService, SubscriptionServiceStatistics stats,
                                WebSocketSessionRef sessionRef, int cmdId) {
        super(serviceId, wsService, entityService, localSubscriptionService, attributesService, stats, sessionRef, cmdId);
        this.subToEntityIdMap = new ConcurrentHashMap<>();
    }

    /** 按 {@link #buildEntityDataQuery} 查本页，写入 {@link #data}。 */
    @Override
    public void fetchData() {
        this.data = findEntityData();
    }

    protected PageData<EntityData> findEntityData() {
        PageData<EntityData> result = entityService.findEntityDataByQuery(getTenantId(), getCustomerId(), buildEntityDataQuery());
        if (log.isTraceEnabled()) {
            result.getData().forEach(ed -> {
                log.trace("[{}][{}] EntityData: {}", getSessionId(), getCmdId(), ed);
            });
        }
        return result;
    }

    /** pageLink.dynamic 为 true 时 Service 才会挂定时刷新。 */
    @Override
    public boolean isDynamic() {
        return query != null && query.getPageLink().isDynamic();
    }

    /**
     * 动态刷新：再查一遍。实体 id 集合没变则什么都不做（遥测增量走柜子，不必整页推）。
     * 有进有出才 {@link #doUpdate}。
     */
    @Override
    protected synchronized void update() {
        PageData<EntityData> newData = findEntityData();
        Map<EntityId, EntityData> oldDataMap;
        if (data != null && !data.getData().isEmpty()) {
            oldDataMap = data.getData().stream().collect(Collectors.toMap(EntityData::getEntityId, Function.identity(), (a, b) -> a));
        } else {
            oldDataMap = Collections.emptyMap();
        }
        Map<EntityId, EntityData> newDataMap = newData.getData().stream().collect(Collectors.toMap(EntityData::getEntityId, Function.identity(), (a, b) -> a));
        if (oldDataMap.size() == newDataMap.size() && oldDataMap.keySet().equals(newDataMap.keySet())) {
            log.trace("[{}][{}] No updates to entity data found", sessionRef.getSessionId(), cmdId);
        } else {
            this.data = newData;
            doUpdate(newDataMap);
        }
    }

    /**
     * 本页实体集合已变。子类应：给离开的实体退订、给新来的实体补订，并推 {@code EntityDataUpdate}。
     *
     * @param newDataMap 新页 entityId → 行数据
     */
    protected abstract void doUpdate(Map<EntityId, EntityData> newDataMap);

    /** 实际丢给 {@link EntityService} 的 query（告警表会把 AlarmDataQuery 转成 EntityDataQuery）。 */
    protected abstract EntityDataQuery buildEntityDataQuery();

    public List<EntityData> getEntitiesData() {
        return data.getData();
    }

    /** 先清实体级内部订阅，再清父类的动态值订阅。 */
    @Override
    public void clearSubscriptions() {
        clearEntitySubscriptions();
        super.clearSubscriptions();
    }

    /**
     * 取消 {@link #subToEntityIdMap} 里全部内部订阅（latest/时序/属性）。
     * 同一 cmdId 带新子命令重建前、ctx stop 时调用。
     */
    public void clearEntitySubscriptions() {
        if (subToEntityIdMap != null) {
            for (Integer subId : subToEntityIdMap.keySet()) {
                localSubscriptionService.cancelSubscription(getTenantId(), getSessionId(), subId);
            }
            subToEntityIdMap.clear();
        }
    }

    /**
     * 为本页每个实体、按 key 类型（遥测 / 各 scope 属性）建 latest 订阅。
     * 这就是「一条 cmd 占柜子很多格」的入口之一。
     */
    public void createLatestValuesSubscriptions(List<EntityKey> keys) {
        createSubscriptions(keys, true, 0, 0);
    }

    /** 按已查到的各实体 key 最后时间戳建时序订阅，增量不当 latest 列推。 */
    public void createTimeSeriesSubscriptions(Map<EntityData, Map<String, Long>> entityKeyStates, long startTs, long endTs) {
        createTimeSeriesSubscriptions(entityKeyStates, startTs, endTs, false);
    }

    /**
     * @param resultToLatestValues true 时后续点走 latest 推送通道（聚合时序窗口订增量时用）
     */
    public void createTimeSeriesSubscriptions(Map<EntityData, Map<String, Long>> entityKeyStates, long startTs, long endTs, boolean resultToLatestValues) {
        entityKeyStates.forEach((entityData, keyStates) -> {
            int subIdx = sessionRef.getSessionSubIdSeq().incrementAndGet();
            subToEntityIdMap.put(subIdx, entityData.getEntityId());
            localSubscriptionService.addSubscription(
                    createTsSub(entityData, subIdx, false, startTs, endTs, keyStates, resultToLatestValues), sessionRef);
        });
    }

    /** 本页每个实体 × 每种 EntityKeyType 一条订阅，全部 add 进柜子。 */
    private void createSubscriptions(List<EntityKey> keys, boolean latestValues, long startTs, long endTs) {
        Map<EntityKeyType, List<EntityKey>> keysByType = getEntityKeyByTypeMap(keys);
        for (EntityData entityData : data.getData()) {
            List<TbSubscription> entitySubscriptions = addSubscriptions(entityData, keysByType, latestValues, startTs, endTs);
            entitySubscriptions.forEach(subscription -> localSubscriptionService.addSubscription(subscription, sessionRef));
        }
    }

    protected Map<EntityKeyType, List<EntityKey>> getEntityKeyByTypeMap(List<EntityKey> keys) {
        Map<EntityKeyType, List<EntityKey>> keysByType = new HashMap<>();
        keys.forEach(key -> keysByType.computeIfAbsent(key.getType(), k -> new ArrayList<>()).add(key));
        return keysByType;
    }

    /**
     * 为单个实体按 key 类型各领一个 {@code sessionSubIdSeq}，写入 {@link #subToEntityIdMap} 后构造订阅对象（尚未 add）。
     */
    protected List<TbSubscription> addSubscriptions(EntityData entityData, Map<EntityKeyType, List<EntityKey>> keysByType, boolean latestValues, long startTs, long endTs) {
        List<TbSubscription> subscriptionList = new ArrayList<>();
        keysByType.forEach((keysType, keysList) -> {
            int subIdx = sessionRef.getSessionSubIdSeq().incrementAndGet();
            subToEntityIdMap.put(subIdx, entityData.getEntityId());
            switch (keysType) {
                case TIME_SERIES:
                    subscriptionList.add(createTsSub(entityData, subIdx, keysList, latestValues, startTs, endTs));
                    break;
                case CLIENT_ATTRIBUTE:
                    subscriptionList.add(createAttrSub(entityData, subIdx, keysType, TbAttributeSubscriptionScope.CLIENT_SCOPE, keysList));
                    break;
                case SHARED_ATTRIBUTE:
                    subscriptionList.add(createAttrSub(entityData, subIdx, keysType, TbAttributeSubscriptionScope.SHARED_SCOPE, keysList));
                    break;
                case SERVER_ATTRIBUTE:
                    subscriptionList.add(createAttrSub(entityData, subIdx, keysType, TbAttributeSubscriptionScope.SERVER_SCOPE, keysList));
                    break;
                case ATTRIBUTE:
                    subscriptionList.add(createAttrSub(entityData, subIdx, keysType, TbAttributeSubscriptionScope.ANY_SCOPE, keysList));
                    break;
            }
        });
        return subscriptionList;
    }

    private TbSubscription createAttrSub(EntityData entityData, int subIdx, EntityKeyType keysType, TbAttributeSubscriptionScope scope, List<EntityKey> subKeys) {
        Map<String, Long> keyStates = buildKeyStats(entityData, keysType, subKeys, true);
        log.trace("[{}][{}][{}] Creating attributes subscription for [{}] with keys: {}", serviceId, cmdId, subIdx, entityData.getEntityId(), keyStates);
        return TbAttributeSubscription.builder()
                .serviceId(serviceId)
                .sessionId(sessionRef.getSessionId())
                .subscriptionId(subIdx)
                .tenantId(sessionRef.getSecurityCtx().getTenantId())
                .entityId(entityData.getEntityId())
                .updateProcessor((sub, subscriptionUpdate) -> sendWsMsg(sub.getSessionId(), subscriptionUpdate, keysType))
                .queryTs(createdTime)
                .allKeys(false)
                .keyStates(keyStates)
                .scope(scope)
                .build();
    }

    private TbSubscription createTsSub(EntityData entityData, int subIdx, List<EntityKey> subKeys, boolean latestValues, long startTs, long endTs) {
        Map<String, Long> keyStates = buildKeyStats(entityData, EntityKeyType.TIME_SERIES, subKeys, latestValues);
        if (!latestValues && entityData.getTimeseries() != null) {
            entityData.getTimeseries().forEach((k, v) -> {
                long ts = Arrays.stream(v).map(TsValue::getTs).max(Long::compareTo).orElse(0L);
                log.trace("[{}][{}] Updating key: {} with ts: {}", serviceId, cmdId, k, ts);
                if (!Aggregation.NONE.equals(getCurrentAggregation()) && ts < endTs) {
                    ts = endTs;
                }
                keyStates.put(k, ts);
            });
        }
        return createTsSub(entityData, subIdx, latestValues, startTs, endTs, keyStates);
    }

    private TbTimeSeriesSubscription createTsSub(EntityData entityData, int subIdx, boolean latestValues, long startTs, long endTs, Map<String, Long> keyStates) {
        return createTsSub(entityData, subIdx, latestValues, startTs, endTs, keyStates, latestValues);
    }

    private TbTimeSeriesSubscription createTsSub(EntityData entityData, int subIdx, boolean latestValues, long startTs, long endTs, Map<String, Long> keyStates, boolean resultToLatestValues) {
        log.trace("[{}][{}][{}] Creating time-series subscription for [{}] with keys: {}", serviceId, cmdId, subIdx, entityData.getEntityId(), keyStates);
        return TbTimeSeriesSubscription.builder()
                .serviceId(serviceId)
                .sessionId(sessionRef.getSessionId())
                .subscriptionId(subIdx)
                .tenantId(sessionRef.getSecurityCtx().getTenantId())
                .entityId(entityData.getEntityId())
                .updateProcessor((sub, subscriptionUpdate) -> sendWsMsg(sub.getSessionId(), subscriptionUpdate, EntityKeyType.TIME_SERIES, resultToLatestValues))
                .queryTs(createdTime)
                .allKeys(false)
                .keyStates(keyStates)
                .latestValues(latestValues)
                .startTime(startTs)
                .endTime(endTs)
                .build();
    }

    /** 属性增量默认当 latest 列推（resultToLatestValues=true）。 */
    private void sendWsMsg(String sessionId, TelemetrySubscriptionUpdate subscriptionUpdate, EntityKeyType keyType) {
        sendWsMsg(sessionId, subscriptionUpdate, keyType, true);
    }

    /**
     * 内部订阅的 keyStates：先全部填 0，latest 模式再用当前行已有 ts 覆盖，避免把快照里已有的点再推一遍。
     */
    private Map<String, Long> buildKeyStats(EntityData entityData, EntityKeyType keysType, List<EntityKey> subKeys, boolean latestValues) {
        Map<String, Long> keyStates = new HashMap<>();
        subKeys.forEach(key -> keyStates.put(key.getKey(), 0L));
        if (latestValues && entityData.getLatest() != null) {
            Map<String, TsValue> currentValues = entityData.getLatest().get(keysType);
            if (currentValues != null) {
                currentValues.forEach((k, v) -> {
                    if (subKeys.contains(new EntityKey(keysType, k))) {
                        log.trace("[{}][{}] Updating key: {} with ts: {}", serviceId, cmdId, k, v.getTs());
                        keyStates.put(k, v.getTs());
                    }
                });
            }
        }
        return keyStates;
    }

    /**
     * 柜子增量到达。子类用 {@link #subToEntityIdMap} 找回实体，决定推进 latest 列还是 timeseries 列。
     *
     * @param resultToLatestValues true：当成表格单元格最新值；false：当成时序曲线上的点
     */
    abstract void sendWsMsg(String sessionId, TelemetrySubscriptionUpdate subscriptionUpdate, EntityKeyType keyType, boolean resultToLatestValues);

    /** 当前时序命令的聚合类型；无时序命令时为 NONE。创建时序订阅算 keyStates 截止 ts 时用。 */
    protected abstract Aggregation getCurrentAggregation();

}
