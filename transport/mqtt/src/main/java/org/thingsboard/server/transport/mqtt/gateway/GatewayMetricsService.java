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
package org.thingsboard.server.transport.mqtt.gateway;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.common.transport.TransportServiceCallback;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.queue.scheduler.SchedulerComponent;
import org.thingsboard.server.transport.mqtt.TbMqttTransportComponent;
import org.thingsboard.server.common.msg.gateway.metrics.GatewayMetadata;
import org.thingsboard.server.transport.mqtt.gateway.metrics.GatewayMetricsState;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


/**
 * 网关度量服务。
 * <p>
 * 负责接收网关上报的度量数据（GatewayMetadata），在内存中按网关聚合统计，
 * 并定期（可配置间隔）将统计结果作为遥测数据发送到 ThingsBoard 平台。
 * 每个网关对应一个 GatewayMetricsState 对象，管理该网关下各连接器的度量。
 */
@Slf4j
@Service
@TbMqttTransportComponent
public class GatewayMetricsService {

    /** 遥测中用于存储网关度量数据的键名 */
    public static final String GATEWAY_METRICS = "gatewayMetrics";

    /** 度量上报间隔，单位秒，默认60秒 */
    @Value("${transport.mqtt.gateway_metrics_report_interval_sec:60}")
    private int metricsReportIntervalSec;

    /** 调度器，用于定时执行上报任务 */
    @Autowired
    private SchedulerComponent scheduler;

    /** 传输服务，用于将遥测数据发送到核心服务 */
    @Autowired
    private TransportService transportService;

    /** 网关设备ID到其度量状态的映射，支持并发访问 */
    private Map<DeviceId, GatewayMetricsState> states = new ConcurrentHashMap<>();

    @PostConstruct
    private void init() {
        scheduler.scheduleAtFixedRate(this::reportMetrics, metricsReportIntervalSec, metricsReportIntervalSec, TimeUnit.SECONDS);
    }

    /**
     * 处理来自网关的度量数据。
     * 根据网关ID获取或创建对应的度量状态，并更新数据。
     *
     * @param sessionInfo     当前网关会话信息
     * @param gatewayId       网关设备ID
     * @param data            网关上报的度量数据列表
     * @param serverReceiveTs 服务端接收到数据的时间戳
     */
    public void process(TransportProtos.SessionInfoProto sessionInfo, DeviceId gatewayId, List<GatewayMetadata> data, long serverReceiveTs) {
        states.computeIfAbsent(gatewayId, k -> new GatewayMetricsState(sessionInfo)).update(data, serverReceiveTs);
    }

    /**
     * 当网关会话更新时调用，更新对应网关度量状态中的会话信息。
     * 例如会话重新建立后，sessionInfo 可能发生变化。
     *
     * @param sessionInfo 新的会话信息
     * @param gatewayId   网关设备ID
     */
    public void onDeviceUpdate(TransportProtos.SessionInfoProto sessionInfo, DeviceId gatewayId) {
        var state = states.get(gatewayId);
        if (state != null) {
            state.updateSessionInfo(sessionInfo);
        }
    }

    /**
     * 当网关设备被删除时调用，移除对应的度量状态。
     *
     * @param deviceId 被删除的网关设备ID
     */
    public void onDeviceDelete(DeviceId deviceId) {
        states.remove(deviceId);
    }

    /**
     * 定时上报任务：收集所有网关的度量结果，并发送遥测。
     * 为了避免长时间持有锁影响性能，采用“交换map”的方式：将当前 states 赋值给局部变量，
     * 并立即新建一个空的 ConcurrentHashMap 供后续使用，然后处理旧的 map 中的数据。
     */
    public void reportMetrics() {
        if (states.isEmpty()) {
            return;
        }
        Map<DeviceId, GatewayMetricsState> statesToReport = states;
        states = new ConcurrentHashMap<>();

        long ts = System.currentTimeMillis();

        statesToReport.forEach((gatewayId, state) -> {
            reportMetrics(state, ts);
        });
    }

    /**
     * 上报单个网关的度量结果。
     * 从状态中获取所有连接器的结果，转换为 JSON 字符串，然后构造遥测消息发送。
     *
     * @param state 网关度量状态
     * @param ts    上报时间戳（毫秒）
     */
    private void reportMetrics(GatewayMetricsState state, long ts) {
        if (state.isEmpty()) {
            return;
        }
        var result = state.getStateResult();
        var kvProto = TransportProtos.KeyValueProto.newBuilder()
                .setKey(GATEWAY_METRICS)
                .setType(TransportProtos.KeyValueType.JSON_V)
                .setJsonV(JacksonUtil.toString(result))
                .build();

        TransportProtos.TsKvListProto tsKvList = TransportProtos.TsKvListProto.newBuilder()
                .setTs(ts)
                .addKv(kvProto)
                .build();

        TransportProtos.PostTelemetryMsg telemetryMsg = TransportProtos.PostTelemetryMsg.newBuilder()
                .addTsKvList(tsKvList)
                .build();

        // 通过传输服务将遥测消息发送出去（使用空回调）
        transportService.process(state.getSessionInfo(), telemetryMsg, TransportServiceCallback.EMPTY);
    }

}
