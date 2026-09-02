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
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.kv.Aggregation;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.query.*;
import org.thingsboard.server.dao.attributes.AttributesService;
import org.thingsboard.server.dao.entity.EntityService;
import org.thingsboard.server.service.ws.WebSocketService;
import org.thingsboard.server.service.ws.WebSocketSessionRef;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.EntityDataCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.EntityDataUpdate;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.LatestValueCmd;
import org.thingsboard.server.service.ws.telemetry.cmd.v2.TimeSeriesCmd;
import org.thingsboard.server.service.ws.telemetry.sub.TelemetrySubscriptionUpdate;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Dashboard 实体表 / 时序图对应的 ctx，处理 {@link EntityDataCmd}。
 *
 * <p>父类已经：查出本页、按实体拆内部订阅。本类补的是<strong>增量怎么合并再推给前端</strong>：
 * <ul>
 *   <li>latest：和 ctx 里已有单元格比较，丢掉更旧或完全相同的点，只推有变化的 key；</li>
 *   <li>timeseries：同样去重后推点数组，并记下每个 key 最大 ts，避免倒序点。</li>
 * </ul>
 *
 * <h2>动态页 {@link #doUpdate}</h2>
 * 本页实体集合变了：离开的行退订；新来的行——若当前只订了 latest、没订时序——补 latest 订阅。
 * 时序图对新实体不在这里补订（注释写明 widgets 会 re-init 另发命令）。
 *
 * <p>{@link #initialDataSent} 区分「第一次推整页 PageData」和「之后只推变化的实体列表」。
 *
 * @see DefaultTbEntityDataSubscriptionService#handleCmd(WebSocketSessionRef, EntityDataCmd)
 * @see TbAbstractDataSubCtx
 */
@Slf4j
public class TbEntityDataSubCtx extends TbAbstractDataSubCtx<EntityDataQuery> {

    /**
     * 是否已向该 cmdId 推过第一包（整页）。
     * false 时 Service 组 {@code EntityDataUpdate} 带完整 PageData；之后只带 update 列表。
     */
    @Getter
    @Setter
    private volatile boolean initialDataSent;

    /** 最近一次命令里的时序子命令；{@link #getCurrentAggregation} 和动态页是否补时序订阅用。 */
    private TimeSeriesCmd curTsCmd;

    /** 最近一次 latest 子命令；动态页出现新实体时按这里的 keys 补订。 */
    private LatestValueCmd latestValueCmd;

    /** 本订阅最多追踪的实体数，下发给前端做截断提示。 */
    @Getter
    private final int maxEntitiesPerDataSubscription;

    /**
     * 每个实体当前已知的最新遥测点，专供 {@link #sendTsWsMsg} 去重。
     * 与 {@code EntityData.latest} 分开存：推完时序后行上的 timeseries 会被清掉，这里还要留着比 ts。
     */
    private Map<EntityId, Map<String, TsValue>> latestTsEntityData;

    public TbEntityDataSubCtx(String serviceId, WebSocketService wsService, EntityService entityService,
                              TbLocalSubscriptionService localSubscriptionService, AttributesService attributesService,
                              SubscriptionServiceStatistics stats, WebSocketSessionRef sessionRef, int cmdId, int maxEntitiesPerDataSubscription) {
        super(serviceId, wsService, entityService, localSubscriptionService, attributesService, stats, sessionRef, cmdId);
        this.maxEntitiesPerDataSubscription = maxEntitiesPerDataSubscription;
    }

    /** 拉本页后立刻缓存各实体 latest 遥测，后面时序增量好做对比。 */
    @Override
    public void fetchData() {
        super.fetchData();
        this.updateLatestTsData(this.data);
    }

    /**
     * 柜子回调入口：用内部 subscriptionId 找回实体。
     * 找不到说明动态刷新已经退订，丢弃这条 stale 更新。
     */
    @Override
    protected void sendWsMsg(String sessionId, TelemetrySubscriptionUpdate subscriptionUpdate, EntityKeyType keyType, boolean resultToLatestValues) {
        EntityId entityId = subToEntityIdMap.get(subscriptionUpdate.getSubscriptionId());
        if (entityId != null) {
            log.trace("[{}][{}][{}][{}] Received subscription update: {}", sessionId, cmdId, subscriptionUpdate.getSubscriptionId(), keyType, subscriptionUpdate);
            if (resultToLatestValues) {
                sendLatestWsMsg(entityId, sessionId, subscriptionUpdate, keyType);
            } else {
                sendTsWsMsg(entityId, sessionId, subscriptionUpdate, keyType);
            }
        } else {
            log.trace("[{}][{}][{}][{}] Received stale subscription update: {}", sessionId, cmdId, subscriptionUpdate.getSubscriptionId(), keyType, subscriptionUpdate);
        }
    }

    @Override
    protected Aggregation getCurrentAggregation() {
        return (this.curTsCmd == null || this.curTsCmd.getAgg() == null) ? Aggregation.NONE : this.curTsCmd.getAgg();
    }

    /**
     * 把增量当成表格 latest 单元格：去掉更旧的 ts、相同值的重复，以及「删除通知」以外的空更新；
     * 剩下的写回 ctx 再只推这一行的变化列。
     */
    private void sendLatestWsMsg(EntityId entityId, String sessionId, TelemetrySubscriptionUpdate subscriptionUpdate, EntityKeyType keyType) {
        Map<String, TsValue> latestUpdate = new HashMap<>();
        subscriptionUpdate.getData().forEach((k, v) -> {
            Object[] data = (Object[]) v.get(0);
            latestUpdate.put(k, new TsValue((Long) data[0], (String) data[1]));
        });
        EntityData entityData = getDataForEntity(entityId);
        if (entityData != null && entityData.getLatest() != null) {
            Map<String, TsValue> latestCtxValues = entityData.getLatest().get(keyType);
            log.trace("[{}][{}][{}] Going to compare update with {}", sessionId, cmdId, subscriptionUpdate.getSubscriptionId(), latestCtxValues);
            if (latestCtxValues != null) {
                latestCtxValues.forEach((k, v) -> {
                    TsValue update = latestUpdate.get(k);
                    if (update != null) {
                        // Ignore notifications about deleted keys
                        if (!(update.getTs() == 0 && (update.getValue() == null || update.getValue().isEmpty()))) {
                            if (update.getTs() < v.getTs()) {
                                log.trace("[{}][{}][{}] Removed stale update for key: {} and ts: {}", sessionId, cmdId, subscriptionUpdate.getSubscriptionId(), k, update.getTs());
                                latestUpdate.remove(k);
                            } else if ((update.getTs() == v.getTs() && update.getValue().equals(v.getValue()))) {
                                log.trace("[{}][{}][{}] Removed duplicate update for key: {} and ts: {}", sessionId, cmdId, subscriptionUpdate.getSubscriptionId(), k, update.getTs());
                                latestUpdate.remove(k);
                            }
                        } else {
                            log.trace("[{}][{}][{}] Received deleted notification for: {}", sessionId, cmdId, subscriptionUpdate.getSubscriptionId(), k);
                        }
                    }
                });
                // Setting new values
                latestUpdate.forEach(latestCtxValues::put);
            }
        }
        if (!latestUpdate.isEmpty()) {
            Map<EntityKeyType, Map<String, TsValue>> latestMap = Collections.singletonMap(keyType, latestUpdate);
            entityData = new EntityData(entityId, latestMap, null);
            sendWsMsg(new EntityDataUpdate(cmdId, null, Collections.singletonList(entityData), maxEntitiesPerDataSubscription));
        }
    }

    /**
     * 时序增量：丢掉与缓存完全相同的点；更旧的点目前只打日志、仍留给 UI 合并。
     * 接受的点写入 {@link #latestTsEntityData} 后推 {@code EntityData.timeseries}。
     */
    private void sendTsWsMsg(EntityId entityId, String sessionId, TelemetrySubscriptionUpdate subscriptionUpdate, EntityKeyType keyType) {
        Map<String, List<TsValue>> tsUpdate = new HashMap<>();
        subscriptionUpdate.getData().forEach((k, v) -> {
            Object[] data = (Object[]) v.get(0);
            tsUpdate.computeIfAbsent(k, key -> new ArrayList<>()).add(new TsValue((Long) data[0], (String) data[1]));
        });
        Map<String, TsValue> latestCtxValues = getLatestTsValuesForEntity(entityId);
        log.trace("[{}][{}][{}] Going to compare update with {}", sessionId, cmdId, subscriptionUpdate.getSubscriptionId(), latestCtxValues);
        if (latestCtxValues != null) {
            latestCtxValues.forEach((k, v) -> {
                List<TsValue> updateList = tsUpdate.get(k);
                if (updateList != null) {
                    for (TsValue update : new ArrayList<>(updateList)) {
                        if (update.getTs() < v.getTs()) {
                            log.trace("[{}][{}][{}] Removed stale update for key: {} and ts: {}", sessionId, cmdId, subscriptionUpdate.getSubscriptionId(), k, update.getTs());
                            // Looks like this is redundant feature and our UI is ready to merge the updates.
                            //updateList.remove(update);
                        } else if ((update.getTs() == v.getTs() && update.getValue().equals(v.getValue()))) {
                            log.trace("[{}][{}][{}] Removed duplicate update for key: {} and ts: {}", sessionId, cmdId, subscriptionUpdate.getSubscriptionId(), k, update.getTs());
                            updateList.remove(update);
                        }
                        if (updateList.isEmpty()) {
                            tsUpdate.remove(k);
                        }
                    }
                }
            });
            // Setting new values
            tsUpdate.forEach((k, v) -> {
                Optional<TsValue> maxValue = v.stream().max(Comparator.comparingLong(TsValue::getTs));
                maxValue.ifPresent(max -> latestCtxValues.put(k, max));
            });
        }
        if (!tsUpdate.isEmpty()) {
            Map<String, TsValue[]> tsMap = new HashMap<>();
            tsUpdate.forEach((key, tsValue) -> tsMap.put(key, tsValue.toArray(new TsValue[tsValue.size()])));
            EntityData entityData = new EntityData(entityId, null, tsMap);
            sendWsMsg(new EntityDataUpdate(cmdId, null, Collections.singletonList(entityData), maxEntitiesPerDataSubscription));
        }
    }

    private EntityData getDataForEntity(EntityId entityId) {
        return data.getData().stream().filter(item -> item.getEntityId().equals(entityId)).findFirst().orElse(null);
    }

    private Map<String, TsValue> getLatestTsValuesForEntity(EntityId entityId) {
        return latestTsEntityData.get(entityId);
    }

    /** 用本页 latest 遥测重建去重缓存。fetch / 动态页换集合之后都要调。 */
    private void updateLatestTsData(PageData<EntityData> data) {
        latestTsEntityData = new HashMap<>();
        data.getData().stream().forEach(entityData -> {
            Map<String, TsValue> latestTsMap = new HashMap<>();
            latestTsEntityData.put(entityData.getEntityId(), latestTsMap);
            if (entityData.getLatest() != null) {
                Map<String, TsValue> latestTsValues = entityData.getLatest().get(EntityKeyType.TIME_SERIES);
                if (latestTsValues != null) {
                    latestTsValues.forEach(latestTsMap::put);
                }
            }
        });
    }

    /**
     * 动态页实体集合变化：离开的 subscriptionId 退订；新实体仅在「当前是 latest 订阅、没有时序命令」时补订。
     * 然后把完整新页推给前端（data 有值、update 为 null，让表格重绘行集合）。
     */
    @Override
    public synchronized void doUpdate(Map<EntityId, EntityData> newDataMap) {
        this.updateLatestTsData(this.data);
        List<Integer> subIdsToCancel = new ArrayList<>();
        List<TbSubscription> subsToAdd = new ArrayList<>();
        Set<EntityId> currentSubs = new HashSet<>();
        subToEntityIdMap.forEach((subId, entityId) -> {
            if (!newDataMap.containsKey(entityId)) {
                subIdsToCancel.add(subId);
            } else {
                currentSubs.add(entityId);
            }
        });
        log.trace("[{}][{}] Subscriptions that are invalid: {}", sessionRef.getSessionId(), cmdId, subIdsToCancel);
        subIdsToCancel.forEach(subToEntityIdMap::remove);
        List<EntityData> newSubsList = newDataMap.entrySet().stream().filter(entry -> !currentSubs.contains(entry.getKey())).map(Map.Entry::getValue).collect(Collectors.toList());
        if (!newSubsList.isEmpty()) {
            // NOTE: We ignore the TS subscriptions for new entities here, because widgets will re-init it's content and will create new subscriptions.
            if (curTsCmd == null && latestValueCmd != null) {
                List<EntityKey> keys = latestValueCmd.getKeys();
                if (keys != null && !keys.isEmpty()) {
                    Map<EntityKeyType, List<EntityKey>> keysByType = getEntityKeyByTypeMap(keys);
                    newSubsList.forEach(
                            entity -> {
                                log.trace("[{}][{}] Found new subscription for entity: {}", sessionRef.getSessionId(), cmdId, entity.getEntityId());
                                subsToAdd.addAll(addSubscriptions(entity, keysByType, true, 0, 0));
                            }
                    );
                }
            }
        }
        subIdsToCancel.forEach(subId -> localSubscriptionService.cancelSubscription(getTenantId(), getSessionId(), subId));
        subsToAdd.forEach(subscription -> localSubscriptionService.addSubscription(subscription, sessionRef));
        sendWsMsg(new EntityDataUpdate(cmdId, data, null, maxEntitiesPerDataSubscription));
    }

    /** Service 每次 handleCmd 记下当前子命令，动态刷新补订时要知道订的是 latest 还是时序。 */
    public void setCurrentCmd(EntityDataCmd cmd) {
        curTsCmd = cmd.getTsCmd();
        latestValueCmd = cmd.getLatestCmd();
    }

    @Override
    protected EntityDataQuery buildEntityDataQuery() {
        return query;
    }

}
