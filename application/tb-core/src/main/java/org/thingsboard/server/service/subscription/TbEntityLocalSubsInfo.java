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
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.plugin.ComponentLifecycleEvent;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 实体本地订阅信息管理类
 * <p>
 * 这个类负责管理单个实体的所有本地 WebSocket 订阅信息，包括：
 * 1. 订阅的增删改查操作
 * 2. 订阅状态的维护和变更跟踪
 * 3. 待处理订阅的管理（用于错过更新补偿）
 * 4. 订阅事件的生成和序列号管理
 * <p>
 * 每个实体（设备、资产等）都有一个对应的 TbEntityLocalSubsInfo 实例
 * 用于跟踪该实体的所有本地订阅和订阅状态
 */
@Slf4j
@RequiredArgsConstructor
public class TbEntityLocalSubsInfo {

    @Getter
    private final TenantId tenantId;
    @Getter
    private final EntityId entityId;

    /**
     * 该实体的所有订阅数据集合，使用线程安全的Set实现
     */
    @Getter
    private final Set<TbSubscription<?>> subs = ConcurrentHashMap.newKeySet();

    /**
     * 当前实体的订阅状态信息
     * 用于优化数据分发，避免向没有订阅的类型发送数据
     * <p>
     * 状态包括：
     * - 是否有通知订阅
     * - 是否有告警订阅
     * - 属性订阅的键集合（或标记为全键订阅）
     * - 时间序列订阅的键集合（或标记为全键订阅）
     */
    private volatile TbSubscriptionsInfo state = new TbSubscriptionsInfo();

    /**
     * 待处理订阅映射：序列号 -> 订阅数据集合
     * <p>
     * 当新订阅建立时，会将其添加到待处理集合中，等待集群确认
     * 只有在收到集群回调后，才会实际处理这些订阅的错过更新
     */
    private final Map<Integer, Set<TbSubscription<?>>> pendingSubs = new ConcurrentHashMap<>();

    /**
     * 待处理时间序列事件的序列号
     */
    @Getter
    @Setter
    private int pendingTimeSeriesEvent;

    /**
     * 待处理时间序列事件的时间戳
     */
    @Getter
    @Setter
    private long pendingTimeSeriesEventTs;

    /**
     * 待处理属性事件的序列号
     */
    @Getter
    @Setter
    private int pendingAttributesEvent;

    /**
     * 待处理属性事件的时间戳
     */
    @Getter
    @Setter
    private long pendingAttributesEventTs;

    /**
     * 事件序列号，用于保证订阅事件的顺序和唯一性
     * 每次生成事件时递增，用于集群间的状态同步
     */
    private int seqNumber = 0;

    /**
     * 添加订阅到实体
     * <p>
     * 流程：
     * 1. 将订阅添加到订阅集合
     * 2. 更新订阅状态信息
     * 3. 根据状态变化生成相应的事件
     *
     * @param subscription 要添加的订阅
     * @return 订阅事件（null表示没有状态变化）
     */
    public TbEntitySubEvent add(TbSubscription<?> subscription) {
        log.trace("[{}][{}][{}] Adding: {}", tenantId, entityId, subscription.getSubscriptionId(), subscription);
        // 检查是否是第一个订阅（实体订阅的创建）
        boolean created = subs.isEmpty();
        subs.add(subscription);
        // 如果是第一个订阅，则状态对象是新的，直接拿来使用；否则复制当前状态用来使用（写时复制）
        TbSubscriptionsInfo newState = created ? state : state.copy();
        boolean stateChanged = false;
        // 根据订阅类型更新状态
        switch (subscription.getType()) {
            case NOTIFICATIONS:
            case NOTIFICATIONS_COUNT:
                if (!newState.notifications) {
                    newState.notifications = true;
                    stateChanged = true;
                }
                break;
            case ALARMS:
                if (!newState.alarms) {
                    newState.alarms = true;
                    stateChanged = true;
                }
                break;
            case ATTRIBUTES:
                var attrSub = (TbAttributeSubscription) subscription;
                if (!newState.attrAllKeys) {
                    if (attrSub.isAllKeys()) {
                        // 标记为全键属性订阅
                        newState.attrAllKeys = true;
                        stateChanged = true;
                    } else {
                        // 添加特定键到属性订阅集合
                        if (newState.attrKeys == null) {
                            newState.attrKeys = new HashSet<>(attrSub.getKeyStates().keySet());
                            stateChanged = true;
                        } else if (newState.attrKeys.addAll(attrSub.getKeyStates().keySet())) {
                            stateChanged = true;
                        }
                    }
                }
                break;
            case TIMESERIES:
                var tsSub = (TbTimeSeriesSubscription) subscription;
                if (!newState.tsAllKeys) {
                    if (tsSub.isAllKeys()) {
                        // 标记为全键时间序列订阅
                        newState.tsAllKeys = true;
                        stateChanged = true;
                    } else {
                        // 添加特定键到时间序列订阅集合
                        if (newState.tsKeys == null) {
                            newState.tsKeys = new HashSet<>(tsSub.getKeyStates().keySet());
                            stateChanged = true;
                        } else if (newState.tsKeys.addAll(tsSub.getKeyStates().keySet())) {
                            stateChanged = true;
                        }
                    }
                }
                break;
        }
        // 如果状态发生变化，更新状态并生成相应事件
        if (stateChanged) {
            state = newState;
        }
        if (created) {
            // 第一个订阅，生成创建事件
            return toEvent(ComponentLifecycleEvent.CREATED);
        } else if (stateChanged) {
            // 状态变化，生成更新事件
            return toEvent(ComponentLifecycleEvent.UPDATED);
        } else {
            // 没有状态变化，返回null
            return null;
        }
    }

    /**
     * 移除单个订阅
     *
     * @param sub 要移除的订阅
     * @return 订阅事件（null表示没有状态变化或订阅不存在）
     */
    public TbEntitySubEvent remove(TbSubscription<?> sub) {
        log.trace("[{}][{}][{}] Removing: {}", tenantId, entityId, sub.getSubscriptionId(), sub);
        // 尝试移除订阅，如果订阅不存在则返回null
        if (!subs.remove(sub)) {
            return null;
        }
        // 如果移除后没有订阅了，生成删除事件
        if (isEmpty()) {
            return toEvent(ComponentLifecycleEvent.DELETED);
        }
        // 重新计算状态
        TbSubscriptionType type = sub.getType();
        TbSubscriptionsInfo newState = state.copy();
        clearState(newState, type);
        return updateState(Set.of(type), newState);
    }

    /**
     * 批量移除订阅
     *
     * @param subsToRemove 要移除的订阅列表
     * @return 订阅事件（null表示没有状态变化）
     */
    public TbEntitySubEvent removeAll(List<? extends TbSubscription<?>> subsToRemove) {
        Set<TbSubscriptionType> changedTypes = new HashSet<>();
        TbSubscriptionsInfo newState = state.copy();
        // 遍历所有要移除的订阅
        for (TbSubscription<?> sub : subsToRemove) {
            log.trace("[{}][{}][{}] Removing: {}", tenantId, entityId, sub.getSubscriptionId(), sub);
            if (!subs.remove(sub)) {
                // 订阅不存在，跳过
                continue;
            }
            // 如果移除后没有订阅了，生成删除事件
            if (isEmpty()) {
                return toEvent(ComponentLifecycleEvent.DELETED);
            }
            TbSubscriptionType type = sub.getType();
            if (changedTypes.contains(type)) {
                // 该类型已经处理过，跳过
                continue;
            }

            // 清除该类型的状态
            clearState(newState, type);
            changedTypes.add(type);
        }

        return updateState(changedTypes, newState);
    }

    /**
     * 清除指定订阅类型的状态
     *
     * @param state 状态对象
     * @param type 订阅类型
     */
    private void clearState(TbSubscriptionsInfo state, TbSubscriptionType type) {
        switch (type) {
            case NOTIFICATIONS:
            case NOTIFICATIONS_COUNT:
                state.notifications = false;
                break;
            case ALARMS:
                state.alarms = false;
                break;
            case ATTRIBUTES:
                state.attrAllKeys = false;
                state.attrKeys = null;
                break;
            case TIMESERIES:
                state.tsAllKeys = false;
                state.tsKeys = null;
        }
    }

    /**
     * 更新状态并检查是否需要生成事件
     *
     * @param updatedTypes 发生变化的订阅类型集合
     * @param newState 新的状态对象
     * @return 订阅事件（null表示状态没有实际变化）
     */
    private TbEntitySubEvent updateState(Set<TbSubscriptionType> updatedTypes, TbSubscriptionsInfo newState) {
        // 重新计算剩余订阅的状态
        for (TbSubscription<?> subscription : subs) {
            TbSubscriptionType type = subscription.getType();
            if (!updatedTypes.contains(type)) {
                // 该类型没有变化，跳过
                continue;
            }
            switch (type) {
                case NOTIFICATIONS:
                case NOTIFICATIONS_COUNT:
                    if (!newState.notifications) {
                        newState.notifications = true;
                    }
                    break;
                case ALARMS:
                    if (!newState.alarms) {
                        newState.alarms = true;
                    }
                    break;
                case ATTRIBUTES:
                    var attrSub = (TbAttributeSubscription) subscription;
                    if (!newState.attrAllKeys && attrSub.isAllKeys()) {
                        newState.attrAllKeys = true;
                        continue;
                    }
                    if (newState.attrKeys == null) {
                        newState.attrKeys = new HashSet<>(attrSub.getKeyStates().keySet());
                    } else {
                        newState.attrKeys.addAll(attrSub.getKeyStates().keySet());
                    }
                    break;
                case TIMESERIES:
                    var tsSub = (TbTimeSeriesSubscription) subscription;
                    if (!newState.tsAllKeys && tsSub.isAllKeys()) {
                        newState.tsAllKeys = true;
                        continue;
                    }
                    if (newState.tsKeys == null) {
                        newState.tsKeys = new HashSet<>(tsSub.getKeyStates().keySet());
                    } else {
                        newState.tsKeys.addAll(tsSub.getKeyStates().keySet());
                    }
                    break;
            }
        }
        if (newState.equals(state)) {
            return null;
        } else {
            this.state = newState;
            return toEvent(ComponentLifecycleEvent.UPDATED);
        }
    }

    public TbEntitySubEvent toEvent(ComponentLifecycleEvent type) {
        seqNumber++;
        var result = TbEntitySubEvent.builder().tenantId(tenantId).entityId(entityId).type(type).seqNumber(seqNumber);
        if (!ComponentLifecycleEvent.DELETED.equals(type)) {
            result.info(state.copy(seqNumber));
        }
        return result.build();
    }

    public boolean isNf() {
        return state.notifications;
    }


    public boolean isEmpty() {
        return subs.isEmpty();
    }

    /**
     * 注册待处理订阅（用于错过更新补偿机制）
     * <p>
     * 当新订阅建立时，可能已经错过了一些数据更新。
     * 这个方法将订阅标记为待处理状态，等待集群确认后再查询历史数据。
     *
     * @param subscription 待处理的订阅
     * @param event 相关的事件（当订阅状态没有发生改变的时候，为null）
     * @return 如果需要立即检查错过更新，返回订阅；否则返回null
     */
    public TbSubscription<?> registerPendingSubscription(TbSubscription<?> subscription, TbEntitySubEvent event) {
        if (TbSubscriptionType.ATTRIBUTES.equals(subscription.getType())) {
            if (event != null) {
                // 事件存在，表示订阅导致状态变化了
                log.trace("[{}][{}] Registering new pending attributes subscription event: {} for subscription: {}", tenantId, entityId, event.getSeqNumber(), subscription.getSubscriptionId());
                // 记录属性事件的序列号和时间戳（直接覆盖之前的状态）
                pendingAttributesEvent = event.getSeqNumber();
                pendingAttributesEventTs = System.currentTimeMillis();
                // 将订阅添加到待处理集合
                pendingSubs.computeIfAbsent(pendingAttributesEvent, e -> new HashSet<>()).add(subscription);
            } else if (pendingAttributesEvent > 0) {
                // 订阅没有导致状态变化，但已经有待处理的属性事件
                log.trace("[{}][{}] Registering pending attributes subscription {} for event: {} ", tenantId, entityId, subscription.getSubscriptionId(), pendingAttributesEvent);
                // 使用现有的待处理事件
                pendingSubs.computeIfAbsent(pendingAttributesEvent, e -> new HashSet<>()).add(subscription);
            } else {
                // 订阅没有导致状态变化，也没有待处理事件，立即返回订阅对象
                return subscription;
            }
        } else if (subscription instanceof TbTimeSeriesSubscription) {
            if (event != null) {
                // 事件存在，表示订阅导致状态变化了
                log.trace("[{}][{}] Registering new pending time-series subscription event: {} for subscription: {}", tenantId, entityId, event.getSeqNumber(), subscription.getSubscriptionId());
                // 记录时间序列事件的序列号和时间戳
                pendingTimeSeriesEvent = event.getSeqNumber();
                pendingTimeSeriesEventTs = System.currentTimeMillis();
                // 将订阅添加到待处理集合
                pendingSubs.computeIfAbsent(pendingTimeSeriesEvent, e -> new HashSet<>()).add(subscription);
            } else if (pendingTimeSeriesEvent > 0) {
                // 订阅没有导致状态变化，但已经有待处理的属性事件
                log.trace("[{}][{}] Registering pending time-series subscription {} for event: {} ", tenantId, entityId, subscription.getSubscriptionId(), pendingTimeSeriesEvent);
                // 使用现有的待处理事件
                pendingSubs.computeIfAbsent(pendingTimeSeriesEvent, e -> new HashSet<>()).add(subscription);
            } else {
                // 订阅没有导致状态变化，也没有待处理事件，立即返回订阅对象
                return subscription;
            }
        }
        // 订阅已注册为待处理，等待集群回调
        return null;
    }

    public Set<TbSubscription<?>> clearPendingSubscriptions(int seqNumber) {
        if (pendingTimeSeriesEvent == seqNumber) {
            pendingTimeSeriesEvent = 0;
            pendingTimeSeriesEventTs = 0L;
        } else if (pendingAttributesEvent == seqNumber) {
            pendingAttributesEvent = 0;
            pendingAttributesEventTs = 0L;
        }
        return pendingSubs.remove(seqNumber);
    }
}
