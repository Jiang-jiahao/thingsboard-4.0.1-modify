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
package org.thingsboard.server.transport.mqtt.gateway.metrics;

import lombok.Getter;
import org.thingsboard.server.common.msg.gateway.metrics.GatewayMetadata;
import org.thingsboard.server.gen.transport.TransportProtos;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 网关度量状态管理类。
 * <p>
 * 针对单个网关设备，维护该网关下多个连接器（connectors）的度量数据。
 * 每个连接器的度量包括计数、网关延迟和传输延迟的统计（总和、最小、最大）。
 * 该类线程安全，通过显式锁控制并发更新。
 */
public class GatewayMetricsState {

    /**
     * 连接器名称 -> 该连接器的度量状态
     */
    private final Map<String, ConnectorMetricsState> connectors;

    /**
     * 用于并发控制的重入锁
     */
    private final Lock updateLock;

    /**
     * 当前网关的会话信息（包含节点ID、设备ID等），可能随会话更新而改变
     */
    @Getter
    private volatile TransportProtos.SessionInfoProto sessionInfo;

    public GatewayMetricsState(TransportProtos.SessionInfoProto sessionInfo) {
        this.connectors = new HashMap<>();
        this.updateLock = new ReentrantLock();
        this.sessionInfo = sessionInfo;
    }

    /**
     * 更新网关的会话信息（例如当会话重新建立时）
     * @param sessionInfo 新的会话信息
     */
    public void updateSessionInfo(TransportProtos.SessionInfoProto sessionInfo) {
        this.sessionInfo = sessionInfo;
    }

    /**
     * 批量更新网关下各连接器的度量数据。
     * 遍历传入的度量列表，对每个连接器调用其内部状态更新方法。
     * 使用锁确保并发安全。
     *
     * @param metricsData    网关上报的度量数据列表
     * @param serverReceiveTs 服务端接收到数据的时间戳（用于计算传输延迟）
     */
    public void update(List<GatewayMetadata> metricsData, long serverReceiveTs) {
        updateLock.lock();
        try {
            metricsData.forEach(data -> {
                connectors.computeIfAbsent(data.connector(), k -> new ConnectorMetricsState()).update(data, serverReceiveTs);
            });
        } finally {
            updateLock.unlock();
        }
    }

    /**
     * 获取当前累积的所有连接器的度量结果，并清空内部状态。
     * 此方法通常由定时任务调用，用于上报数据。
     *
     * @return 连接器名称到其度量结果的映射
     */
    public Map<String, ConnectorMetricsResult> getStateResult() {
        Map<String, ConnectorMetricsResult> result = new HashMap<>();
        updateLock.lock();
        try {
            connectors.forEach((name, state) -> result.put(name, state.getResult()));
            connectors.clear();
        } finally {
            updateLock.unlock();
        }

        return result;
    }

    public boolean isEmpty() {
        return connectors.isEmpty();
    }


    /**
     * 单个连接器的度量状态内部类。
     * 维护该连接器的计数、延迟总和以及最小/最大延迟。
     */
    private static class ConnectorMetricsState {

        /** 接收到的度量记录条数 */
        private final AtomicInteger count;

        /** 网关内部延迟（publishedTs - receivedTs）的总和 */
        private final AtomicLong gwLatencySum;

        /** 传输延迟（serverReceiveTs - publishedTs）的总和 */
        private final AtomicLong transportLatencySum;

        /** 最小网关延迟 */
        private volatile long minGwLatency;

        /** 最大网关延迟 */
        private volatile long maxGwLatency;

        /** 最小传输延迟 */
        private volatile long minTransportLatency;

        /** 最大传输延迟 */
        private volatile long maxTransportLatency;

        private ConnectorMetricsState() {
            this.count = new AtomicInteger(0);
            this.gwLatencySum = new AtomicLong(0);
            this.transportLatencySum = new AtomicLong(0);
        }

        private void update(GatewayMetadata metricsData, long serverReceiveTs) {
            long gwLatency = metricsData.publishedTs() - metricsData.receivedTs();
            long transportLatency = serverReceiveTs - metricsData.publishedTs();
            count.incrementAndGet();
            gwLatencySum.addAndGet(gwLatency);
            transportLatencySum.addAndGet(transportLatency);
            if (minGwLatency == 0 || minGwLatency > gwLatency) {
                minGwLatency = gwLatency;
            }
            if (maxGwLatency < gwLatency) {
                maxGwLatency = gwLatency;
            }
            if (minTransportLatency == 0 || minTransportLatency > transportLatency) {
                minTransportLatency = transportLatency;
            }
            if (maxTransportLatency < transportLatency) {
                maxTransportLatency = transportLatency;
            }
        }

        private ConnectorMetricsResult getResult() {
            long count = this.count.get();
            long avgGwLatency = gwLatencySum.get() / count;
            long avgTransportLatency = transportLatencySum.get() / count;
            return new ConnectorMetricsResult(avgGwLatency, minGwLatency, maxGwLatency, avgTransportLatency, minTransportLatency, maxTransportLatency);
        }
    }

    public record ConnectorMetricsResult(long avgGwLatency, long minGwLatency, long maxGwLatency,
                                         long avgTransportLatency, long minTransportLatency, long maxTransportLatency) {
    }

}
