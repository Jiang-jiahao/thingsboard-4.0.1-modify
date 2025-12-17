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

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.thingsboard.server.common.data.ApiFeature;
import org.thingsboard.server.common.data.ApiUsageRecordKey;
import org.thingsboard.server.common.data.ApiUsageState;
import org.thingsboard.server.common.data.ApiUsageStateValue;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.msg.tools.SchedulerUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * API使用状态基类 - 负责跟踪和管理API使用统计
 * 这是一个抽象基类，用于处理API使用率监控和限制功能
 */
public abstract class BaseApiUsageState {

    // 当前周期（本月）的API使用统计值映射表，键为API使用记录键，值为使用量
    private final Map<ApiUsageRecordKey, Long> currentCycleValues = new ConcurrentHashMap<>();

    // 当前小时的API使用统计值映射表，用于实时监控
    private final Map<ApiUsageRecordKey, Long> currentHourValues = new ConcurrentHashMap<>();

    // 用于存储各服务上报的最新度量值（gauge），键为API使用记录键，值为服务ID到最新值的映射
    private final Map<ApiUsageRecordKey, Map<String, Long>> lastGaugesByServiceId = new HashMap<>();

    // 记录每个度量键（gauge key）的最后上报周期时间戳
    private final Map<ApiUsageRecordKey, Long> gaugesReportCycles = new HashMap<>();

    // API使用状态实体对象
    @Getter
    @Setter
    private ApiUsageState apiUsageState;

    // 当前周期（本月）开始时间戳
    @Getter
    private volatile long currentCycleTs;

    // 下一个周期开始时间戳
    @Getter
    private volatile long nextCycleTs;

    // 当前小时开始时间戳
    @Getter
    private volatile long currentHourTs;

    // 度量值上报时间间隔
    @Setter
    private long gaugeReportInterval;

    public BaseApiUsageState(ApiUsageState apiUsageState) {
        this.apiUsageState = apiUsageState;
        this.currentCycleTs = SchedulerUtils.getStartOfCurrentMonth();
        this.nextCycleTs = SchedulerUtils.getStartOfNextMonth();
        this.currentHourTs = SchedulerUtils.getStartOfCurrentHour();
    }

    /**
     * 计算并更新API使用统计
     * @param key
     * @param value 本次使用的值
     * @param serviceId
     * @return
     */
    public StatsCalculationResult calculate(ApiUsageRecordKey key, long value, String serviceId) {
        // 获取当前周期（当月）累计值
        long currentValue = get(key);
        // 获取当前小时累计值
        long currentHourlyValue = getHourly(key);

        StatsCalculationResult result;
        // 计数器（Counter）：直接累加，用于累计性统计（如API调用次数）
        // 度量值（Gauge）：收集各服务上报的值，定期计算总和或最大值，用于瞬时值统计（如并发连接数）
        if (key.isCounter()) {
            // 计数器类型：直接累加
            result = StatsCalculationResult.builder()
                    .newValue(currentValue + value).valueChanged(true)
                    .newHourlyValue(currentHourlyValue + value).hourlyValueChanged(true)
                    .build();
        } else {
            // 度量值类型：需要特殊处理，取各个服务上报值的总和或最大值
            Long newGaugeValue = calculateGauge(key, value, serviceId);
            long newValue = newGaugeValue != null ? newGaugeValue : currentValue;
            long newHourlyValue = newGaugeValue != null ? Math.max(newGaugeValue, currentHourlyValue) : currentHourlyValue;
            result = StatsCalculationResult.builder()
                    .newValue(newValue).valueChanged(newValue != currentValue || !currentCycleValues.containsKey(key))
                    .newHourlyValue(newHourlyValue).hourlyValueChanged(newHourlyValue != currentHourlyValue || !currentHourValues.containsKey(key))
                    .build();
        }

        set(key, result.getNewValue());
        setHourly(key, result.getNewHourlyValue());
        return result;
    }

    private Long calculateGauge(ApiUsageRecordKey key, long value, String serviceId) {
        Map<String, Long> lastByServiceId = lastGaugesByServiceId.computeIfAbsent(key, k -> {
            gaugesReportCycles.put(key, System.currentTimeMillis());
            return new HashMap<>();
        });
        lastByServiceId.put(serviceId, value);

        // 检查是否到达报告间隔
        Long gaugeReportCycle = gaugesReportCycles.get(key);
        if (gaugeReportCycle <= System.currentTimeMillis() - gaugeReportInterval) {
            long newValue = lastByServiceId.values().stream().mapToLong(Long::longValue).sum();
            lastGaugesByServiceId.remove(key);
            gaugesReportCycles.remove(key);
            return newValue;
        } else {
            return null;
        }
    }

    public void set(ApiUsageRecordKey key, Long value) {
        currentCycleValues.put(key, value);
    }

    public long get(ApiUsageRecordKey key) {
        return currentCycleValues.getOrDefault(key, 0L);
    }

    public void setHourly(ApiUsageRecordKey key, Long value) {
        currentHourValues.put(key, value);
    }

    public long getHourly(ApiUsageRecordKey key) {
        return currentHourValues.getOrDefault(key, 0L);
    }

    public void setHour(long currentHourTs) {
        this.currentHourTs = currentHourTs;
        currentHourValues.clear();
        lastGaugesByServiceId.clear();
        gaugesReportCycles.clear();
    }

    public void setCycles(long currentCycleTs, long nextCycleTs) {
        this.currentCycleTs = currentCycleTs;
        this.nextCycleTs = nextCycleTs;
        currentCycleValues.clear();
    }

    public void onRepartitionEvent() {
        lastGaugesByServiceId.clear();
        gaugesReportCycles.clear();
    }

    public ApiUsageStateValue getFeatureValue(ApiFeature feature) {
        switch (feature) {
            case TRANSPORT:
                return apiUsageState.getTransportState();
            case RE:
                return apiUsageState.getReExecState();
            case DB:
                return apiUsageState.getDbStorageState();
            case JS:
                return apiUsageState.getJsExecState();
            case TBEL:
                return apiUsageState.getTbelExecState();
            case EMAIL:
                return apiUsageState.getEmailExecState();
            case SMS:
                return apiUsageState.getSmsExecState();
            case ALARM:
                return apiUsageState.getAlarmExecState();
            default:
                return ApiUsageStateValue.ENABLED;
        }
    }

    public boolean setFeatureValue(ApiFeature feature, ApiUsageStateValue value) {
        ApiUsageStateValue currentValue = getFeatureValue(feature);
        switch (feature) {
            case TRANSPORT:
                apiUsageState.setTransportState(value);
                break;
            case RE:
                apiUsageState.setReExecState(value);
                break;
            case DB:
                apiUsageState.setDbStorageState(value);
                break;
            case JS:
                apiUsageState.setJsExecState(value);
                break;
            case TBEL:
                apiUsageState.setTbelExecState(value);
                break;
            case EMAIL:
                apiUsageState.setEmailExecState(value);
                break;
            case SMS:
                apiUsageState.setSmsExecState(value);
                break;
            case ALARM:
                apiUsageState.setAlarmExecState(value);
                break;
        }
        return !currentValue.equals(value);
    }

    public abstract EntityType getEntityType();

    public TenantId getTenantId() {
        return getApiUsageState().getTenantId();
    }

    public EntityId getEntityId() {
        return getApiUsageState().getEntityId();
    }

    @Override
    public String toString() {
        return "BaseApiUsageState{" +
                "apiUsageState=" + apiUsageState +
                ", currentCycleTs=" + currentCycleTs +
                ", nextCycleTs=" + nextCycleTs +
                ", currentHourTs=" + currentHourTs +
                '}';
    }

    @Data
    @Builder
    public static class StatsCalculationResult {
        private final long newValue;
        private final boolean valueChanged;
        private final long newHourlyValue;
        private final boolean hourlyValueChanged;
    }

}
