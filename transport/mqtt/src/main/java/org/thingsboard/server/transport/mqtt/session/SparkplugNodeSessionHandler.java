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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.protobuf.Descriptors;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.leshan.core.ResponseCode;
import org.springframework.util.CollectionUtils;
import org.thingsboard.server.common.adaptor.AdaptorException;
import org.thingsboard.server.common.adaptor.JsonConverter;
import org.thingsboard.server.common.adaptor.ProtoConverter;
import org.thingsboard.server.common.data.device.profile.MqttDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.exception.ThingsboardErrorCode;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.transport.auth.GetOrCreateDeviceFromGatewayResponse;
import org.thingsboard.server.gen.transport.TransportApiProtos;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.gen.transport.mqtt.SparkplugBProto;
import org.thingsboard.server.transport.mqtt.MqttTransportHandler;
import org.thingsboard.server.transport.mqtt.util.sparkplug.MetricDataType;
import org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugTopic;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugConnectionState.ONLINE;
import static org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugMessageType.DBIRTH;
import static org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugMessageType.NBIRTH;
import static org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugMetricUtil.createMetric;
import static org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugMetricUtil.fromSparkplugBMetricToKeyValueProto;
import static org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugMetricUtil.validatedValueByTypeMetric;
import static org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugTopicUtil.parseTopicSubscribe;

/**
 * Sparkplug 节点会话处理器，专门处理Sparkplug协议中的节点（Node）及其下设备的消息。
 * 支持NBIRTH、DBIRTH、NDEATH、DDEATH等Sparkplug消息类型。
 * Created by nickAS21 on 12.12.22
 */
@Slf4j
public class SparkplugNodeSessionHandler extends AbstractGatewaySessionHandler<SparkplugDeviceSessionContext> {

    /**
     * 当前节点的Sparkplug主题信息
     */
    @Getter
    private final SparkplugTopic sparkplugTopicNode;

    /**
     * 节点出生时携带的指标缓存
     */
    @Getter
    private final Map<String, SparkplugBProto.Payload.Metric> nodeBirthMetrics;

    /**
     * 父MQTT处理器，用于发送RPC等
     */
    private final MqttTransportHandler parent;

    public SparkplugNodeSessionHandler(MqttTransportHandler parent, DeviceSessionCtx deviceSessionCtx, UUID sessionId,
                                       boolean overwriteDevicesActivity, SparkplugTopic sparkplugTopicNode) {
        super(deviceSessionCtx, sessionId, overwriteDevicesActivity);
        this.parent = parent;
        this.sparkplugTopicNode = sparkplugTopicNode;
        this.nodeBirthMetrics = new ConcurrentHashMap<>();
    }

    /**
     * 设置节点出生时的指标列表，存储到Map中便于后续查找。
     */
    public void setNodeBirthMetrics(java.util.List<org.thingsboard.server.gen.transport.mqtt.SparkplugBProto.Payload.Metric> metrics) {
        this.nodeBirthMetrics.putAll(metrics.stream()
                .collect(Collectors.toMap(SparkplugBProto.Payload.Metric::getName, metric -> metric)));
    }

    /**
     * 将Sparkplug负载转换为ThingsBoard的遥测消息（用于动态Proto描述符的情况）。
     */
    public TransportProtos.PostTelemetryMsg convertToPostTelemetry(MqttDeviceAwareSessionContext ctx, MqttPublishMessage inbound) throws AdaptorException {
        DeviceSessionCtx deviceSessionCtx = (DeviceSessionCtx) ctx;
        byte[] bytes = getBytes(inbound.payload());
        Descriptors.Descriptor telemetryDynamicMsgDescriptor = ProtoConverter.validateDescriptor(deviceSessionCtx.getTelemetryDynamicMsgDescriptor());
        try {
            return JsonConverter.convertToTelemetryProto(JsonParser.parseString(ProtoConverter.dynamicMsgToJson(bytes, telemetryDynamicMsgDescriptor)));
        } catch (Exception e) {
            log.debug("Failed to decode post telemetry request", e);
            throw new AdaptorException(e);
        }
    }

    /**
     * 处理Sparkplug协议中的属性（Attributes）和遥测（Telemetry）消息。
     * 根据主题类型（节点或设备）和消息类型（NBIRTH/DBIRTH等）进行相应处理。
     *
     * @param msgId            MQTT消息ID
     * @param sparkplugBProto  Sparkplug负载
     * @param topic            解析后的主题
     */
    public void onAttributesTelemetryProto(int msgId, SparkplugBProto.Payload sparkplugBProto, SparkplugTopic topic) throws AdaptorException, ThingsboardException {
        String deviceName = topic.getNodeDeviceName();
        checkDeviceName(deviceName);

        ListenableFuture<MqttDeviceAwareSessionContext> contextListenableFuture;
        if (topic.isNode()) {
            // 如果是节点消息，当前会话就是节点本身
            if (topic.isType(NBIRTH)) {
                // 节点出生：发送ONLINE状态遥测，并缓存节点指标
                sendSparkplugStateOnTelemetry(this.deviceSessionCtx.getSessionInfo(), deviceName, ONLINE,
                        sparkplugBProto.getTimestamp());
                setNodeBirthMetrics(sparkplugBProto.getMetricsList());
            }
            contextListenableFuture = Futures.immediateFuture(this.deviceSessionCtx);
        } else {
            // 如果是设备消息，需要先获取或创建设备会话
            ListenableFuture<SparkplugDeviceSessionContext> deviceCtx = onDeviceConnectProto(topic);
            contextListenableFuture = Futures.transform(deviceCtx, ctx -> {
                if (topic.isType(DBIRTH)) {
                    // 设备出生：发送ONLINE状态遥测，并缓存设备指标
                    sendSparkplugStateOnTelemetry(ctx.getSessionInfo(), deviceName, ONLINE,
                            sparkplugBProto.getTimestamp());
                    ctx.setDeviceBirthMetrics(sparkplugBProto.getMetricsList());
                }
                return ctx;
            }, MoreExecutors.directExecutor());
        }
        // 获取设备配置中指定的属性指标名称列表
        Set<String> attributesMetricNames = ((MqttDeviceProfileTransportConfiguration) deviceSessionCtx
                .getDeviceProfile().getProfileData().getTransportConfiguration()).getSparkplugAttributesMetricNames();
        // 处理属性（如果指标在属性列表中）
        if (attributesMetricNames != null) {
            List<TransportApiProtos.AttributesMsg> attributesMsgList = convertToPostAttributes(sparkplugBProto, attributesMetricNames, deviceName);
            onDeviceAttributesProto(contextListenableFuture, msgId, attributesMsgList, deviceName);
        }
        // 处理遥测（排除属性指标）
        List<TransportProtos.PostTelemetryMsg> postTelemetryMsgList = convertToPostTelemetry(sparkplugBProto, attributesMetricNames, topic.getType().name());
        onDeviceTelemetryProto(contextListenableFuture, msgId, postTelemetryMsgList, deviceName);
    }

    /**
     * 异步发送遥测数据到子设备。
     */
    public void onDeviceTelemetryProto(ListenableFuture<MqttDeviceAwareSessionContext> contextListenableFuture,
                                       int msgId, List<TransportProtos.PostTelemetryMsg> postTelemetryMsgList, String deviceName) {
        process(contextListenableFuture, deviceCtx -> {
                    for (TransportProtos.PostTelemetryMsg telemetryMsg : postTelemetryMsgList) {
                        try {
                            processPostTelemetryMsg(deviceCtx, telemetryMsg, deviceName, msgId);
                        } catch (Throwable e) {
                            log.warn("[{}][{}] Failed to convert telemetry: {}", gateway.getDeviceId(), deviceName, telemetryMsg, e);
                            ackOrClose(msgId);
                        }
                    }
                },
                t -> log.debug("[{}] Failed to process device telemetry command: {}", sessionId, deviceName, t));
    }

    /**
     * 异步发送属性数据到子设备。
     */
    private void onDeviceAttributesProto(ListenableFuture<MqttDeviceAwareSessionContext> contextListenableFuture, int msgId,
                                         List<TransportApiProtos.AttributesMsg> attributesMsgList, String deviceName) throws AdaptorException {
        try {
            if (CollectionUtils.isEmpty(attributesMsgList)) {
                log.debug("[{}] Devices attributes keys list is empty for: [{}]", sessionId, gateway.getDeviceId());
            }
            process(contextListenableFuture, deviceCtx -> {
                        for (TransportApiProtos.AttributesMsg attributesMsg : attributesMsgList) {
                            TransportProtos.PostAttributeMsg kvListProto = attributesMsg.getMsg();
                            try {
                                TransportProtos.PostAttributeMsg postAttributeMsg = ProtoConverter.validatePostAttributeMsg(kvListProto);
                                processPostAttributesMsg(deviceCtx, postAttributeMsg, deviceName, msgId);
                            } catch (Throwable e) {
                                log.warn("[{}][{}] Failed to process device attributes command: {}", gateway.getDeviceId(), deviceName, kvListProto, e);
                            }
                        }
                    },
                    t -> log.debug("[{}] Failed to process device attributes command: {}", sessionId, deviceName, t));
        } catch (RuntimeException e) {
            throw new AdaptorException(e);
        }
    }

    /**
     * 处理Sparkplug订阅消息（例如节点订阅主题），用于处理RPC和属性更新订阅。
     */
    public void handleSparkplugSubscribeMsg(List<Integer> grantedQoSList, MqttTopicSubscription subscription,
                                            MqttQoS reqQoS) throws ThingsboardException {
        SparkplugTopic sparkplugTopic = parseTopicSubscribe(subscription.topicFilter());
        if (sparkplugTopic.getGroupId() == null) {
            // TODO SUBSCRIBE NameSpace
        } else if (sparkplugTopic.getType() == null) {
            // TODO SUBSCRIBE GroupId
        } else if (sparkplugTopic.isNode()) {
            // SUBSCRIBE Node
            parent.processAttributesRpcSubscribeSparkplugNode(grantedQoSList, reqQoS);
        } else {
            // SUBSCRIBE Device - DO NOTHING, WE HAVE ALREADY SUBSCRIBED.
            // TODO: track that node subscribed to # or to particular device.
        }
    }

    /**
     * 处理子设备断开连接（Sparkplug设备死亡消息）。
     */
    public void onDeviceDisconnect(MqttPublishMessage mqttMsg, String deviceName) throws AdaptorException {
        try {
            processOnDisconnect(mqttMsg, deviceName);
        } catch (RuntimeException e) {
            throw new AdaptorException(e);
        }
    }

    /**
     * 根据Sparkplug主题异步获取或创建设备会话。
     */
    private ListenableFuture<SparkplugDeviceSessionContext> onDeviceConnectProto(SparkplugTopic topic) throws ThingsboardException {
        try {
            String deviceType = this.gateway.getDeviceType() + " device";
            return onDeviceConnect(topic.getNodeDeviceName(), deviceType);
        } catch (RuntimeException e) {
            log.error("Failed Sparkplug Device connect proto!", e);
            throw new ThingsboardException(e, ThingsboardErrorCode.BAD_REQUEST_PARAMS);
        }
    }

    /**
     * 将Sparkplug指标列表转换为ThingsBoard遥测消息列表。
     * 排除属性指标（如果提供了attributesMetricNames），并为DBIRTH类型添加序列号指标。
     */
    private List<TransportProtos.PostTelemetryMsg> convertToPostTelemetry(SparkplugBProto.Payload sparkplugBProto, Set<String> attributesMetricNames, String topicTypeName) throws AdaptorException {
        try {
            List<TransportProtos.PostTelemetryMsg> msgs = new ArrayList<>();
            for (SparkplugBProto.Payload.Metric protoMetric : sparkplugBProto.getMetricsList()) {
                if (attributesMetricNames == null || !matches(attributesMetricNames, protoMetric)) {
                    long ts = protoMetric.getTimestamp();
                    // 对于"bdSeq"指标，加上主题类型前缀避免冲突
                    String key = "bdSeq".equals(protoMetric.getName()) ?
                            topicTypeName + " " + protoMetric.getName() : protoMetric.getName();
                    Optional<TransportProtos.KeyValueProto> keyValueProtoOpt = fromSparkplugBMetricToKeyValueProto(key, protoMetric);
                    keyValueProtoOpt.ifPresent(kvProto -> msgs.add(postTelemetryMsgCreated(kvProto, ts)));
                }
            }

            // 如果是DBIRTH消息，额外添加序列号作为遥测
            if (DBIRTH.name().equals(topicTypeName)) {
                TransportProtos.KeyValueProto.Builder keyValueProtoBuilder = TransportProtos.KeyValueProto.newBuilder();
                keyValueProtoBuilder.setKey(topicTypeName + " " + "seq");
                keyValueProtoBuilder.setType(TransportProtos.KeyValueType.LONG_V);
                keyValueProtoBuilder.setLongV(sparkplugBProto.getSeq());
                msgs.add(postTelemetryMsgCreated(keyValueProtoBuilder.build(), sparkplugBProto.getTimestamp()));
            }
            return msgs;
        } catch (IllegalStateException | JsonSyntaxException | ThingsboardException e) {
            log.error("Failed to decode post telemetry request", e);
            throw new AdaptorException(e);
        }
    }

    /**
     * 将Sparkplug指标列表转换为属性消息列表。
     * 只包含在attributesMetricNames中匹配的指标。
     */
    private List<TransportApiProtos.AttributesMsg> convertToPostAttributes(SparkplugBProto.Payload sparkplugBProto,
                                                                           Set<String> attributesMetricNames,
                                                                           String deviceName) throws AdaptorException {
        try {
            List<TransportApiProtos.AttributesMsg> msgs = new ArrayList<>();
            for (SparkplugBProto.Payload.Metric protoMetric : sparkplugBProto.getMetricsList()) {
                if (matches(attributesMetricNames, protoMetric)) {
                    TransportApiProtos.AttributesMsg.Builder deviceAttributesMsgBuilder = TransportApiProtos.AttributesMsg.newBuilder();
                    Optional<TransportProtos.PostAttributeMsg> msgOpt = getPostAttributeMsg(protoMetric);
                    if (msgOpt.isPresent()) {
                        deviceAttributesMsgBuilder.setDeviceName(deviceName);
                        deviceAttributesMsgBuilder.setMsg(msgOpt.get());
                        msgs.add(deviceAttributesMsgBuilder.build());
                    }
                }
            }
            return msgs;
        } catch (IllegalStateException | JsonSyntaxException | ThingsboardException e) {
            log.error("Failed to decode post telemetry request", e);
            throw new AdaptorException(e);
        }
    }

    /**
     * 判断指标名称是否匹配属性指标过滤器（支持精确匹配和通配符*）。
     */
    private boolean matches(Set<String> attributesMetricNames, SparkplugBProto.Payload.Metric protoMetric) {
        String metricName = protoMetric.getName();
        for (String attributeMetricFilter : attributesMetricNames) {
            if (metricName.equals(attributeMetricFilter) ||
                    (attributeMetricFilter.endsWith("*") && metricName.startsWith(
                            attributeMetricFilter.substring(0, attributeMetricFilter.length() - 1)))) {
                return true;
            }
        }
        return false;
    }

    /**
     * 从单个Sparkplug指标创建PostAttributeMsg。
     */
    private Optional<TransportProtos.PostAttributeMsg> getPostAttributeMsg(SparkplugBProto.Payload.Metric protoMetric) throws ThingsboardException {
        Optional<TransportProtos.KeyValueProto> keyValueProtoOpt = fromSparkplugBMetricToKeyValueProto(protoMetric.getName(), protoMetric);
        if (keyValueProtoOpt.isPresent()) {
            TransportProtos.PostAttributeMsg.Builder builder = TransportProtos.PostAttributeMsg.newBuilder();
            builder.addKv(keyValueProtoOpt.get());
            builder.setShared(true);
            return Optional.of(builder.build());
        }
        return Optional.empty();
    }

    /**
     * 根据ThingsBoard的遥测数据创建Sparkplug格式的MQTT发布消息（用于RPC响应）。
     */
    public Optional<MqttPublishMessage> createSparkplugMqttPublishMsg(TransportProtos.TsKvProto tsKvProto,
                                                                      String sparkplugTopic,
                                                                      SparkplugBProto.Payload.Metric metricBirth) {
        try {
            long ts = tsKvProto.getTs();
            MetricDataType metricDataType = MetricDataType.fromInteger(metricBirth.getDatatype());
            Optional value = validatedValueByTypeMetric(tsKvProto.getKv(), metricDataType);
            if (value.isPresent()) {
                SparkplugBProto.Payload.Builder cmdPayload = SparkplugBProto.Payload.newBuilder()
                        .setTimestamp(ts);
                cmdPayload.addMetrics(createMetric(value.get(), ts, tsKvProto.getKv().getKey(), metricDataType));
                byte[] payloadInBytes = cmdPayload.build().toByteArray();
                return Optional.of(getPayloadAdaptor().createMqttPublishMsg(deviceSessionCtx, sparkplugTopic, payloadInBytes));
            } else {
                log.trace("DeviceId: [{}] tenantId: [{}] sessionId:[{}] Failed to convert device attributes [{}] response to MQTT sparkplug  msg",
                        deviceSessionCtx.getDeviceInfo().getDeviceId(), deviceSessionCtx.getDeviceInfo().getTenantId(), sessionId, tsKvProto.getKv());
            }
        } catch (Exception e) {
            log.trace("DeviceId: [{}] tenantId: [{}] sessionId:[{}] Failed to convert device attributes response to MQTT sparkplug  msg",
                    deviceSessionCtx.getDeviceInfo().getDeviceId(), deviceSessionCtx.getDeviceInfo().getTenantId(), sessionId, e);
        }
        return Optional.empty();
    }

    /**
     * 创建新的Sparkplug设备会话上下文。
     */
    @Override
    protected SparkplugDeviceSessionContext newDeviceSessionCtx(GetOrCreateDeviceFromGatewayResponse msg) {
        return new SparkplugDeviceSessionContext(this, msg.getDeviceInfo(), msg.getDeviceProfile(), mqttQoSMap, transportService);
    }

    // 以下方法委托给父处理器处理RPC相关操作
    protected void sendToDeviceRpcRequest(MqttMessage payload, TransportProtos.ToDeviceRpcRequestMsg rpcRequest, TransportProtos.SessionInfoProto sessionInfo) {
        parent.sendToDeviceRpcRequest(payload, rpcRequest, sessionInfo);
    }

    protected void sendErrorRpcResponse(TransportProtos.SessionInfoProto sessionInfo, int requestId, ThingsboardErrorCode result, String errorMsg) {
        parent.sendErrorRpcResponse(sessionInfo, requestId, result, errorMsg);
    }

    protected void sendSuccessRpcResponse(TransportProtos.SessionInfoProto sessionInfo, int requestId, ResponseCode result, String successMsg) {
        parent.sendSuccessRpcResponse(sessionInfo, requestId, result, successMsg);
    }

}
