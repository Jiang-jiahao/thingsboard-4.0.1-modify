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
package org.thingsboard.server.transport.mqtt.session;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.adaptor.AdaptorException;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.transport.auth.GetOrCreateDeviceFromGatewayResponse;
import org.thingsboard.server.gen.transport.TransportProtos;

import java.util.Optional;
import java.util.UUID;

/**
 * 标准MQTT网关会话处理器，用于处理普通网关设备（非Sparkplug）的子设备管理。
 * Created by nickAS21 on 26.12.22
 */
@Slf4j
public class GatewaySessionHandler extends AbstractGatewaySessionHandler<GatewayDeviceSessionContext> {

    public GatewaySessionHandler(DeviceSessionCtx deviceSessionCtx, UUID sessionId, boolean overwriteDevicesActivity) {
        super(deviceSessionCtx, sessionId, overwriteDevicesActivity);
    }

    /**
     * 处理子设备连接请求（入口方法），根据负载类型分发到JSON或Protobuf具体处理。
     */
    public void onDeviceConnect(MqttPublishMessage mqttMsg) throws AdaptorException {
        if (isJsonPayloadType()) {
            onDeviceConnectJson(mqttMsg);
        } else {
            onDeviceConnectProto(mqttMsg);
        }
    }

    /**
     * 处理子设备遥测数据（入口方法），根据负载类型分发。
     */
    public void onDeviceTelemetry(MqttPublishMessage mqttMsg) throws AdaptorException {
        int msgId = getMsgId(mqttMsg);
        ByteBuf payload = mqttMsg.payload();
        if (isJsonPayloadType()) {
            onDeviceTelemetryJson(msgId, payload);
        } else {
            onDeviceTelemetryProto(msgId, payload);
        }
    }

    /**
     * 创建新的子设备会话上下文（GatewayDeviceSessionContext）。
     */
    @Override
    protected GatewayDeviceSessionContext newDeviceSessionCtx(GetOrCreateDeviceFromGatewayResponse msg) {
        return new GatewayDeviceSessionContext(this, msg.getDeviceInfo(), msg.getDeviceProfile(), mqttQoSMap, transportService);
    }

    /**
     * 当网关设备本身信息更新时调用，更新覆盖活动时间的标志，并通知指标服务。
     */
    public void onGatewayUpdate(TransportProtos.SessionInfoProto sessionInfo, Device device, Optional<DeviceProfile> deviceProfileOpt) {
        this.onDeviceUpdate(sessionInfo, device, deviceProfileOpt);
        gatewayMetricsService.onDeviceUpdate(sessionInfo, gateway.getDeviceId());
    }

    /**
     * 当网关设备被删除时调用，清理指标服务中的相关数据。
     */
    public void onGatewayDelete(DeviceId deviceId) {
        gatewayMetricsService.onDeviceDelete(deviceId);
    }

}
