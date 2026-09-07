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
 * 本节点上「某一个实体」的全部 WebSocket 订阅汇总。
 *
 * <p>{@link DefaultTbLocalSubscriptionService} 按实体建一份本对象，挂在 {@code subscriptionsByEntityId} 上。
 * 设备上报时按实体找到它，就能知道本节点有哪些连接在盯这个实体。
 * 按连接取消时走另一张表 {@code subscriptionsBySessionId}，不经过本类做主键查找。
 *
 * <h2>两层信息</h2>
 * <ul>
 *   <li>{@link #subs}：逐条订阅（某条 WS 的某个 subId）；</li>
 *   <li>{@link #state}：把所有订阅合并成「本节点对这个实体的兴趣」——要不要告警/通知、属性/遥测订了哪些键。</li>
 * </ul>
 * 集群里的 {@code SubscriptionManagerService} 只关心 {@code state} 变没变。
 * 第二个浏览器再订已经盯着的 {@code temperature}，{@code state} 不变，就不必再发事件。
 *
 * <h2>事件与错过补偿</h2>
 * {@link #add} / {@link #remove} 在兴趣变化时生成带 {@link #seqNumber} 的 {@link TbEntitySubEvent}，
 * 推给负责该实体的 Core。确认回调到达前，新订阅先放进 {@link #pendingSubs}，
 * 避免空档里已经发生的属性/遥测更新漏推。
 *
 * @see DefaultTbLocalSubscriptionService
 * @see TbSubscriptionsInfo
 * @see TbEntitySubEvent
 */
@Slf4j
@RequiredArgsConstructor
public class TbEntityLocalSubsInfo {

    @Getter
    private final TenantId tenantId;
    @Getter
    private final EntityId entityId;

    /**
     * 本节点上盯这个实体的全部订阅对象。增删都在租户锁内进行；
     * 用 ConcurrentHashMap 的 keySet 只是避免迭代时的结构性问题，不能替代外层锁。
     */
    @Getter
    private final Set<TbSubscription<?>> subs = ConcurrentHashMap.newKeySet();

    /**
     * 合并后的兴趣：有没有通知/告警订阅，属性、遥测是全键还是指定键集合。
     * 管理器按这份信息决定上报时要不要把某类数据转到本节点。
     * 写时复制：先改副本，确认有变化再赋回，避免读到半更新状态。
     */
    private volatile TbSubscriptionsInfo state = new TbSubscriptionsInfo();

    /**
     * 等待集群确认的订阅：{@code seqNumber → 等这个序号回调的订阅集合}。
     * 回调 {@link #clearPendingSubscriptions} 取出后再做错过更新检查。
     */
    private final Map<Integer, Set<TbSubscription<?>>> pendingSubs = new ConcurrentHashMap<>();

    /**
     * 当前尚未确认的遥测兴趣变更序号。{@code > 0} 表示还有人在等这次 CREATED/UPDATED 的回调。
     * 回调到达或被更新的序号覆盖后清零。
     */
    @Getter
    @Setter
    private int pendingTimeSeriesEvent;

    /** 登记上述遥测 pending 时的时间戳，便于排查订阅确认是否过慢。 */
    @Getter
    @Setter
    private long pendingTimeSeriesEventTs;

    /** 当前尚未确认的属性兴趣变更序号，含义同 {@link #pendingTimeSeriesEvent}。 */
    @Getter
    @Setter
    private int pendingAttributesEvent;

    /** 登记上述属性 pending 时的时间戳。 */
    @Getter
    @Setter
    private long pendingAttributesEventTs;

    /**
     * 本实体订阅事件的本地序号，每次 {@link #toEvent} 加一。
     * 集群用它把「兴趣变更」和「确认回调」对上，也用来挂 pending。
     */
    private int seqNumber = 0;

    /**
     * 增加一条订阅，并视需要生成集群事件。
     *
     * <p>返回值：
     * <ul>
     *   <li>本实体在本节点的第一条订阅 → {@code CREATED}；</li>
     *   <li>不是第一条，但合并兴趣变了（新类型或新键）→ {@code UPDATED}；</li>
     *   <li>兴趣没变（例如又有人订已经在盯的键）→ {@code null}，调用方不必通知管理器。</li>
     * </ul>
     *
     * @param subscription 已带 sessionId / subId / 实体 / 键信息的订阅
     */
    public TbEntitySubEvent add(TbSubscription<?> subscription) {
        log.trace("[{}][{}][{}] Adding: {}", tenantId, entityId, subscription.getSubscriptionId(), subscription);
        boolean created = subs.isEmpty();
        subs.add(subscription);
        // 第一条可以直接改当前 state；已有订阅则 copy，避免并发读到改到一半的对象。
        TbSubscriptionsInfo newState = created ? state : state.copy();
        boolean stateChanged = false;
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
                // 已经是全键订阅时，再加指定键不会扩大兴趣。
                if (!newState.attrAllKeys) {
                    if (attrSub.isAllKeys()) {
                        newState.attrAllKeys = true;
                        stateChanged = true;
                    } else {
                        if (newState.attrKeys == null) {
                            newState.attrKeys = new HashSet<>(attrSub.getKeyStates().keySet());
                            stateChanged = true;
                        } else if (newState.attrKeys.addAll(attrSub.getKeyStates().keySet())) {
                            // addAll 返回 true 表示集合里出现了新键。
                            stateChanged = true;
                        }
                    }
                }
                break;
            case TIMESERIES:
                var tsSub = (TbTimeSeriesSubscription) subscription;
                if (!newState.tsAllKeys) {
                    if (tsSub.isAllKeys()) {
                        newState.tsAllKeys = true;
                        stateChanged = true;
                    } else {
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
        if (stateChanged) {
            state = newState;
        }
        if (created) {
            return toEvent(ComponentLifecycleEvent.CREATED);
        } else if (stateChanged) {
            return toEvent(ComponentLifecycleEvent.UPDATED);
        } else {
            return null;
        }
    }

    /**
     * 移除单条订阅。
     *
     * <p>对象不在集合里 → {@code null}（重复取消）。
     * 删完后 {@link #subs} 空了 → {@code DELETED}，管理器应停止往本节点转发该实体。
     * 否则先清空该类型在 {@code state} 里的合并结果，再扫剩余订阅重建，有变化才 {@code UPDATED}。
     */
    public TbEntitySubEvent remove(TbSubscription<?> sub) {
        log.trace("[{}][{}][{}] Removing: {}", tenantId, entityId, sub.getSubscriptionId(), sub);
        if (!subs.remove(sub)) {
            return null;
        }
        if (isEmpty()) {
            return toEvent(ComponentLifecycleEvent.DELETED);
        }
        TbSubscriptionType type = sub.getType();
        TbSubscriptionsInfo newState = state.copy();
        clearState(newState, type);
        return updateState(Set.of(type), newState);
    }

    /**
     * 批量移除（关整条 WS 时，同一实体上可能挂了多条订阅）。
     *
     * <p>中途若集合已空，立刻返回 {@code DELETED}，不再扫剩余类型。
     * 同一类型只 {@link #clearState} 一次，最后用 {@link #updateState} 从还在的订阅重建。
     */
    public TbEntitySubEvent removeAll(List<? extends TbSubscription<?>> subsToRemove) {
        Set<TbSubscriptionType> changedTypes = new HashSet<>();
        TbSubscriptionsInfo newState = state.copy();
        for (TbSubscription<?> sub : subsToRemove) {
            log.trace("[{}][{}][{}] Removing: {}", tenantId, entityId, sub.getSubscriptionId(), sub);
            if (!subs.remove(sub)) {
                continue;
            }
            if (isEmpty()) {
                return toEvent(ComponentLifecycleEvent.DELETED);
            }
            TbSubscriptionType type = sub.getType();
            if (changedTypes.contains(type)) {
                continue;
            }

            clearState(newState, type);
            changedTypes.add(type);
        }

        return updateState(changedTypes, newState);
    }

    /**
     * 把某一类型在 {@code state} 里的合并结果清掉，供随后从剩余 {@link #subs} 重建。
     * 不能只把「被删那一条」的键抠掉：不知道别人是否还订着同一个键。
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
     * 仅根据 {@code updatedTypes} 里的类型，用还在的 {@link #subs} 重建 {@code newState}。
     * 其它类型保持副本里原值。重建后与当前 {@link #state} 相等则无事件。
     */
    private TbEntitySubEvent updateState(Set<TbSubscriptionType> updatedTypes, TbSubscriptionsInfo newState) {
        for (TbSubscription<?> subscription : subs) {
            TbSubscriptionType type = subscription.getType();
            if (!updatedTypes.contains(type)) {
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

    /**
     * 生成一条带新序号的事件。DELETED 不再附带 {@code info}（本节点已无兴趣）；
     * CREATED/UPDATED 把当前 {@link #state} 拷贝进去，并写入同一个 seqNumber。
     */
    public TbEntitySubEvent toEvent(ComponentLifecycleEvent type) {
        seqNumber++;
        var result = TbEntitySubEvent.builder().tenantId(tenantId).entityId(entityId).type(type).seqNumber(seqNumber);
        if (!ComponentLifecycleEvent.DELETED.equals(type)) {
            result.info(state.copy(seqNumber));
        }
        return result.build();
    }

    /** 本节点是否有人订了该实体的通知（列表或未读计数）。 */
    public boolean isNf() {
        return state.notifications;
    }


    /** 本节点是否已经没有任何订阅盯这个实体。为空后服务会从 {@code subscriptionsByEntityId} 摘掉本对象。 */
    public boolean isEmpty() {
        return subs.isEmpty();
    }

    /**
     * 决定这条新订阅是立刻做错过更新检查，还是先等集群确认。
     *
     * <p>只处理属性和遥测（这两类才有「订上前后上报空档」问题）。
     *
     * <ul>
     *   <li>{@code event != null}：本次 add 改变了兴趣，把订阅挂到这个 event 的 seqNumber 上，等回调；</li>
     *   <li>{@code event == null} 但同类已有 pending：兴趣没变，可是上一次兴趣变更还没确认，
     *       也挂到那个序号上，避免确认前漏数据；</li>
     *   <li>两者都没有：管理器侧兴趣早已对齐，返回本订阅让调用方马上 {@code checkMissedUpdates}。</li>
     * </ul>
     *
     * @return 需要立刻补查则返回订阅本身；已进入 pending 则返回 {@code null}
     */
    public TbSubscription<?> registerPendingSubscription(TbSubscription<?> subscription, TbEntitySubEvent event) {
        if (TbSubscriptionType.ATTRIBUTES.equals(subscription.getType())) {
            if (event != null) {
                log.trace("[{}][{}] Registering new pending attributes subscription event: {} for subscription: {}", tenantId, entityId, event.getSeqNumber(), subscription.getSubscriptionId());
                pendingAttributesEvent = event.getSeqNumber();
                pendingAttributesEventTs = System.currentTimeMillis();
                pendingSubs.computeIfAbsent(pendingAttributesEvent, e -> new HashSet<>()).add(subscription);
            } else if (pendingAttributesEvent > 0) {
                log.trace("[{}][{}] Registering pending attributes subscription {} for event: {} ", tenantId, entityId, subscription.getSubscriptionId(), pendingAttributesEvent);
                pendingSubs.computeIfAbsent(pendingAttributesEvent, e -> new HashSet<>()).add(subscription);
            } else {
                return subscription;
            }
        } else if (subscription instanceof TbTimeSeriesSubscription) {
            if (event != null) {
                log.trace("[{}][{}] Registering new pending time-series subscription event: {} for subscription: {}", tenantId, entityId, event.getSeqNumber(), subscription.getSubscriptionId());
                pendingTimeSeriesEvent = event.getSeqNumber();
                pendingTimeSeriesEventTs = System.currentTimeMillis();
                pendingSubs.computeIfAbsent(pendingTimeSeriesEvent, e -> new HashSet<>()).add(subscription);
            } else if (pendingTimeSeriesEvent > 0) {
                log.trace("[{}][{}] Registering pending time-series subscription {} for event: {} ", tenantId, entityId, subscription.getSubscriptionId(), pendingTimeSeriesEvent);
                pendingSubs.computeIfAbsent(pendingTimeSeriesEvent, e -> new HashSet<>()).add(subscription);
            } else {
                return subscription;
            }
        }
        return null;
    }

    /**
     * 管理器确认序号 {@code seqNumber} 后调用：清掉对应的 pending 标记，返回等这个序号的订阅集合。
     * 调用方再对这些订阅做错过更新检查。没有挂过该序号则返回 {@code null}。
     */
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
