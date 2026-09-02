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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.data.AttributeScope;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.id.UserId;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
import org.thingsboard.server.common.data.query.*;
import org.thingsboard.server.dao.attributes.AttributesService;
import org.thingsboard.server.dao.entity.EntityService;
import org.thingsboard.server.service.ws.WebSocketService;
import org.thingsboard.server.service.ws.WebSocketSessionRef;
import org.thingsboard.server.service.ws.telemetry.sub.TelemetrySubscriptionUpdate;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;

/**
 * 带 {@link EntityCountQuery} 的 ctx：实体计数、实体表、告警表/计数都从这里长出来。
 *
 * <p>在 {@link TbAbstractSubCtx} 之上增加三件事：
 * <ol>
 *   <li>保存并执行 query（{@link #fetchData} / {@link #update}）；</li>
 *   <li>解析 keyFilters 里的<strong>动态值</strong>（从当前租户/客户/用户的 server 属性取值，而不是写死常量）；
 *       属性变了会失效过滤条件，触发 {@link #update} 重查；</li>
 *   <li>挂/取消 {@link #refreshTask}（Service 侧 dynamic 定时器）。</li>
 * </ol>
 *
 * <h2>动态值在干什么</h2>
 * 过滤条件可以写成「温度 &gt; 当前用户的 threshold 属性」。
 * {@link #setAndResolveQuery} 扫 predicate，把这类 DynamicValue 登记进 {@link #dynamicValues}，
 * 再按来源实体（租户/客户/用户）去柜子里订对应 server 属性。
 * 属性一变：写回 predicate → {@code update()} 重跑 query。
 * 这和「本页设备的遥测订阅」不是同一批内部订阅，id 记在 {@link #subToDynamicValueKeySet}。
 *
 * @param <T> 计数 query 或其子类（实体数据 query、告警 query 等）
 * @see TbAbstractDataSubCtx
 * @see TbEntityCountSubCtx
 */
@Slf4j
public abstract class TbAbstractEntityQuerySubCtx<T extends EntityCountQuery> extends TbAbstractSubCtx {

    /** 按 query 查实体/计数。 */
    protected final EntityService entityService;

    /** 解析动态值时读 server 属性。 */
    protected final AttributesService attributesService;

    /**
     * 为动态值建的内部订阅 id。取消时只清这些，不动子类 {@code subToEntityIdMap} 里的实体遥测订阅。
     */
    protected final Set<Integer> subToDynamicValueKeySet;

    /**
     * 过滤条件里出现过的动态值：同一来源属性可能被多个 predicate 引用，所以 value 是列表。
     * resolve 成功后 {@link DynamicValue#setResolvedValue} 写上当前值，query 才能真正过滤。
     */
    @Getter
    protected final Map<DynamicValueKey, List<DynamicValue>> dynamicValues;

    /** 当前生效的 query。{@link #setAndResolveQuery} 时替换并重新解析动态值。 */
    @Getter
    @Setter
    protected T query;

    /** Service 挂上的动态页/告警窗刷新任务。{@link #cancelTasks} / {@link #stop} 时取消。 */
    @Setter
    protected volatile ScheduledFuture<?> refreshTask;

    public TbAbstractEntityQuerySubCtx(String serviceId, WebSocketService wsService, EntityService entityService, TbLocalSubscriptionService localSubscriptionService,
                                       AttributesService attributesService, SubscriptionServiceStatistics stats, WebSocketSessionRef sessionRef, int cmdId) {
        super(serviceId, wsService, localSubscriptionService, stats, sessionRef, cmdId);
        this.entityService = entityService;
        this.attributesService = attributesService;
        this.subToDynamicValueKeySet = ConcurrentHashMap.newKeySet();
        this.dynamicValues = new ConcurrentHashMap<>();
    }

    /** 按当前 query 查一遍（实体列表或计数），结果留在子类字段里。 */
    public abstract void fetchData();

    /**
     * query 结果可能变了（动态过滤值变了、定时刷新到了）。
     * 子类决定是重查计数还是对比本页实体集合并增删内部订阅。
     */
    protected abstract void update();

    /**
     * 清本层订阅。默认只清动态值那批；{@link TbAbstractDataSubCtx} 会先清实体级订阅再调 super。
     */
    public void clearSubscriptions() {
        clearDynamicValueSubscriptions();
    }

    /**
     * 停止：打标 → 取消刷新任务 → 清掉本 ctx 在柜子里的订阅（含动态值 + 子类实体订阅）。
     */
    public void stop() {
        super.stop();
        cancelTasks();
        clearSubscriptions();
    }

    /**
     * 换上新 query：清空旧动态值、从 keyFilters 收集 DynamicValue、按租户/客户/用户把属性读出来并订上。
     *
     * <p>TODO: 换 query / 连续调用时未先 {@link #clearDynamicValueSubscriptions}。
     * 柜子里旧的动态值属性订阅会留下：打开带动态过滤的实体表时 createSubCtx 与 handleCmd 会 resolve 两次（双订）；
     * 同一 cmdId 换掉动态属性后，旧回调里 {@code dynamicValues.get} 可能为 null 导致 NPE。
     * 关掉会话时 {@link #stop} 会清掉，故不是永久泄漏。修复：方法开头先 clear 再登记。
     */
    public void setAndResolveQuery(T query) {
        dynamicValues.clear();
        this.query = query;
        if (query != null && query.getKeyFilters() != null) {
            for (KeyFilter filter : query.getKeyFilters()) {
                registerDynamicValues(filter.getPredicate());
            }
        }
        resolve(getTenantId(), getCustomerId(), getUserId());
    }

    /**
     * 把已登记的动态值从对应实体的 server 属性读出，再为每个来源实体建一条属性订阅。
     * 读失败只打 info，query 里那些 DynamicValue 保持未解析。
     */
    public void resolve(TenantId tenantId, CustomerId customerId, UserId userId) {
        List<ListenableFuture<DynamicValueKeySub>> futures = new ArrayList<>();
        for (DynamicValueKey key : dynamicValues.keySet()) {
            switch (key.getSourceType()) {
                case CURRENT_TENANT:
                    futures.add(resolveEntityValue(tenantId, tenantId, key));
                    break;
                case CURRENT_CUSTOMER:
                    if (customerId != null && !customerId.isNullUid()) {
                        futures.add(resolveEntityValue(tenantId, customerId, key));
                    }
                    break;
                case CURRENT_USER:
                    if (userId != null && !userId.isNullUid()) {
                        futures.add(resolveEntityValue(tenantId, userId, key));
                    }
                    break;
            }
        }
        try {
            Map<EntityId, Map<String, DynamicValueKeySub>> tmpSubMap = new HashMap<>();
            for (DynamicValueKeySub sub : Futures.successfulAsList(futures).get()) {
                tmpSubMap.computeIfAbsent(sub.getEntityId(), tmp -> new HashMap<>()).put(sub.getKey().getSourceAttribute(), sub);
            }
            for (EntityId entityId : tmpSubMap.keySet()) {
                Map<String, Long> keyStates = new HashMap<>();
                Map<String, DynamicValueKeySub> dynamicValueKeySubMap = tmpSubMap.get(entityId);
                dynamicValueKeySubMap.forEach((k, v) -> keyStates.put(k, v.getLastUpdateTs()));
                int subIdx = sessionRef.getSessionSubIdSeq().incrementAndGet();
                TbAttributeSubscription sub = TbAttributeSubscription.builder()
                        .serviceId(serviceId)
                        .sessionId(sessionRef.getSessionId())
                        .subscriptionId(subIdx)
                        .tenantId(sessionRef.getSecurityCtx().getTenantId())
                        .entityId(entityId)
                        .updateProcessor((subscription, subscriptionUpdate) -> dynamicValueSubUpdate(subscription.getSessionId(), subscriptionUpdate, dynamicValueKeySubMap))
                        .queryTs(createdTime)
                        .allKeys(false)
                        .keyStates(keyStates)
                        .scope(TbAttributeSubscriptionScope.SERVER_SCOPE)
                        .build();
                subToDynamicValueKeySet.add(subIdx);
                localSubscriptionService.addSubscription(sub, sessionRef);
            }
        } catch (InterruptedException | ExecutionException e) {
            log.info("[{}][{}][{}] Failed to resolve dynamic values: {}", tenantId, customerId, userId, dynamicValues.keySet());
        }

    }

    /**
     * 动态值来源属性变更：更新内存中的 DynamicValue，任一 key 真正变了就 {@link #update} 重跑过滤。
     */
    private void dynamicValueSubUpdate(String sessionId, TelemetrySubscriptionUpdate subscriptionUpdate,
                                       Map<String, DynamicValueKeySub> dynamicValueKeySubMap) {
        Map<String, TsValue> latestUpdate = new HashMap<>();
        subscriptionUpdate.getData().forEach((k, v) -> {
            Object[] data = (Object[]) v.get(0);
            latestUpdate.put(k, new TsValue((Long) data[0], (String) data[1]));
        });

        boolean invalidateFilter = false;
        for (Map.Entry<String, TsValue> entry : latestUpdate.entrySet()) {
            String k = entry.getKey();
            TsValue tsValue = entry.getValue();
            DynamicValueKeySub sub = dynamicValueKeySubMap.get(k);
            if (sub.updateValue(tsValue)) {
                invalidateFilter = true;
                updateDynamicValuesByKey(sub, tsValue);
            }
        }

        if (invalidateFilter) {
            update();
        }
    }

    /**
     * 某一个动态属性在柜子里的订阅状态：来源实体 + 上次 ts/值，用于去重。
     */
    @Data
    private static class DynamicValueKeySub {
        private final DynamicValueKey key;
        private final EntityId entityId;
        private long lastUpdateTs;
        private String lastUpdateValue;

        /** ts 更新且值变了才算有效变更（避免相同值反复重查）。 */
        boolean updateValue(TsValue value) {
            if (value.getTs() > lastUpdateTs && (lastUpdateValue == null || !lastUpdateValue.equals(value.getValue()))) {
                this.lastUpdateTs = value.getTs();
                this.lastUpdateValue = value.getValue();
                return true;
            } else {
                return false;
            }
        }
    }

    /** 读一条 server 属性，填进 DynamicValueKeySub，并立刻写回 query 里的 DynamicValue。 */
    private ListenableFuture<DynamicValueKeySub> resolveEntityValue(TenantId tenantId, EntityId entityId, DynamicValueKey key) {
        ListenableFuture<Optional<AttributeKvEntry>> entry = attributesService.find(tenantId, entityId,
                AttributeScope.SERVER_SCOPE, key.getSourceAttribute());
        return Futures.transform(entry, attributeOpt -> {
            DynamicValueKeySub sub = new DynamicValueKeySub(key, entityId);
            if (attributeOpt.isPresent()) {
                AttributeKvEntry attribute = attributeOpt.get();
                sub.setLastUpdateTs(attribute.getLastUpdateTs());
                sub.setLastUpdateValue(attribute.getValueAsString());
                updateDynamicValuesByKey(sub, new TsValue(attribute.getLastUpdateTs(), attribute.getValueAsString()));
            }
            return sub;
        }, MoreExecutors.directExecutor());
    }

    /** 按 predicate 类型把字符串属性转成 String/Double/Boolean，写进所有引用该 key 的 DynamicValue。 */
    @SuppressWarnings("unchecked")
    protected void updateDynamicValuesByKey(DynamicValueKeySub sub, TsValue tsValue) {
        DynamicValueKey dvk = sub.getKey();
        switch (dvk.getPredicateType()) {
            case STRING:
                dynamicValues.get(dvk).forEach(dynamicValue -> dynamicValue.setResolvedValue(tsValue.getValue()));
                break;
            case NUMERIC:
                try {
                    Double dValue = Double.parseDouble(tsValue.getValue());
                    dynamicValues.get(dvk).forEach(dynamicValue -> dynamicValue.setResolvedValue(dValue));
                } catch (NumberFormatException e) {
                    dynamicValues.get(dvk).forEach(dynamicValue -> dynamicValue.setResolvedValue(null));
                }
                break;
            case BOOLEAN:
                Boolean bValue = Boolean.parseBoolean(tsValue.getValue());
                dynamicValues.get(dvk).forEach(dynamicValue -> dynamicValue.setResolvedValue(bValue));
                break;
        }
    }

    /** 递归扫 predicate 树，把未带 userValue 的 DynamicValue 按（类型+来源+属性名）归组。 */
    @SuppressWarnings("unchecked")
    private void registerDynamicValues(KeyFilterPredicate predicate) {
        switch (predicate.getType()) {
            case STRING:
            case NUMERIC:
            case BOOLEAN:
                Optional<DynamicValue> value = getDynamicValueFromSimplePredicate((SimpleKeyFilterPredicate) predicate);
                if (value.isPresent()) {
                    DynamicValue dynamicValue = value.get();
                    DynamicValueKey key = new DynamicValueKey(
                            predicate.getType(),
                            dynamicValue.getSourceType(),
                            dynamicValue.getSourceAttribute());
                    dynamicValues.computeIfAbsent(key, tmp -> new ArrayList<>()).add(dynamicValue);
                }
                break;
            case COMPLEX:
                ((ComplexFilterPredicate) predicate).getPredicates().forEach(this::registerDynamicValues);
        }
    }

    /** 过滤值已由用户手填则不是动态值；只有 dynamicValue 且 userValue 为空才需要去属性里解析。 */
    private Optional<DynamicValue<T>> getDynamicValueFromSimplePredicate(SimpleKeyFilterPredicate<T> predicate) {
        if (predicate.getValue().getUserValue() == null) {
            return Optional.ofNullable(predicate.getValue().getDynamicValue());
        } else {
            return Optional.empty();
        }
    }

    /** 从柜子取消所有「动态过滤值」内部订阅。 */
    protected void clearDynamicValueSubscriptions() {
        if (subToDynamicValueKeySet != null) {
            for (Integer subId : subToDynamicValueKeySet) {
                localSubscriptionService.cancelSubscription(getTenantId(), sessionRef.getSessionId(), subId);
            }
            subToDynamicValueKeySet.clear();
        }
    }

    /**
     * 登记刷新任务。ctx 已 stop 则立刻 cancel，避免取消订阅后定时器还跑。
     */
    public void setRefreshTask(ScheduledFuture<?> task) {
        if (!stopped) {
            this.refreshTask = task;
        } else {
            task.cancel(true);
        }
    }

    /** 取消当前刷新任务（换 query 重挂之前、stop 时）。 */
    public void cancelTasks() {
        if (this.refreshTask != null) {
            log.trace("[{}][{}] Canceling old refresh task", sessionRef.getSessionId(), cmdId);
            this.refreshTask.cancel(true);
        }
    }

    /**
     * 动态值的去重键：谓词类型 + 来源（租户/客户/用户）+ 属性名。
     * 同一键下可以挂多个 DynamicValue 实例（多个 filter 引用同一属性）。
     */
    @Data
    public static class DynamicValueKey {
        @Getter
        private final FilterPredicateType predicateType;
        @Getter
        private final DynamicValueSourceType sourceType;
        @Getter
        private final String sourceAttribute;
    }
}
