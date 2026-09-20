package com.jnks.iot.server.transport.mqtt.session;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.exception.JnksIotErrorCode;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.transport.TransportService;
import com.jnks.iot.server.common.transport.auth.TransportDeviceInfo;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.gen.transport.mqtt.SparkplugBProto;
import com.jnks.iot.server.transport.mqtt.util.sparkplug.SparkplugMessageType;
import com.jnks.iot.server.transport.mqtt.util.sparkplug.SparkplugRpcRequestHeader;
import com.jnks.iot.server.transport.mqtt.util.sparkplug.SparkplugTopic;

import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static com.jnks.iot.server.transport.mqtt.util.sparkplug.SparkplugMetricUtil.getTsKvProto;

@Slf4j
public class SparkplugDeviceSessionContext extends AbstractGatewayDeviceSessionContext<SparkplugNodeSessionHandler> {

    private final Map<String, SparkplugBProto.Payload.Metric> deviceBirthMetrics = new ConcurrentHashMap<>();

    public SparkplugDeviceSessionContext(SparkplugNodeSessionHandler parent,
                                         TransportDeviceInfo deviceInfo,
                                         DeviceProfile deviceProfile,
                                         ConcurrentMap<MqttTopicMatcher,
                                                 Integer> mqttQoSMap,
                                         TransportService transportService) {
        super(parent, deviceInfo, deviceProfile, mqttQoSMap, transportService);
    }

    public  Map<String, SparkplugBProto.Payload.Metric> getDeviceBirthMetrics() {
        return deviceBirthMetrics;
    }

    public void setDeviceBirthMetrics(java.util.List<com.jnks.iot.server.gen.transport.mqtt.SparkplugBProto.Payload.Metric> metrics) {
        this.deviceBirthMetrics.putAll(metrics.stream()
                .collect(Collectors.toMap(SparkplugBProto.Payload.Metric::getName, metric -> metric)));
    }


    @Override
    public void onAttributeUpdate(UUID sessionId, TransportProtos.AttributeUpdateNotificationMsg notification) {
        log.trace("[{}] Received attributes update notification to sparkplug device", sessionId);
        notification.getSharedUpdatedList().forEach(tsKvProto -> {
            if (getDeviceBirthMetrics().containsKey(tsKvProto.getKv().getKey())) {
                SparkplugTopic sparkplugTopic = new SparkplugTopic(parent.getSparkplugTopicNode(),
                        SparkplugMessageType.DCMD, deviceInfo.getDeviceName());
                parent.createSparkplugMqttPublishMsg(tsKvProto,
                        sparkplugTopic.toString(),
                        getDeviceBirthMetrics().get(tsKvProto.getKv().getKey()))
                        .ifPresent(this.parent::writeAndFlush);
            }
        });
    }

    @Override
    public void onToDeviceRpcRequest(UUID sessionId, TransportProtos.ToDeviceRpcRequestMsg rpcRequest) {
        log.trace("[{}] Received RPC Request notification to sparkplug device", sessionId);
        try {
            SparkplugMessageType messageType = SparkplugMessageType.parseMessageType(rpcRequest.getMethodName());
            SparkplugRpcRequestHeader header = JacksonUtil.fromString(rpcRequest.getParams(), SparkplugRpcRequestHeader.class);
            header.setMessageType(messageType.name());
            TransportProtos.TsKvProto tsKvProto = getTsKvProto(header.getMetricName(), header.getValue(), new Date().getTime());
            if (getDeviceBirthMetrics().containsKey(tsKvProto.getKv().getKey())) {
                SparkplugTopic sparkplugTopic = new SparkplugTopic(parent.getSparkplugTopicNode(),
                        messageType, deviceInfo.getDeviceName());
                parent.createSparkplugMqttPublishMsg(tsKvProto,
                        sparkplugTopic.toString(),
                        getDeviceBirthMetrics().get(tsKvProto.getKv().getKey()))
                        .ifPresent(payload -> parent.sendToDeviceRpcRequest(payload, rpcRequest, sessionInfo));
            } else {
                parent.sendErrorRpcResponse(sessionInfo, rpcRequest.getRequestId(),
                        JnksIotErrorCode.BAD_REQUEST_PARAMS, " Failed send To Device Rpc Request: " +
                                rpcRequest.getMethodName() + ". This device does not have a metricName: [" + tsKvProto.getKv().getKey() + "]");
            }
        } catch (JnksIotException e) {
            parent.sendErrorRpcResponse(sessionInfo, rpcRequest.getRequestId(),
                    JnksIotErrorCode.BAD_REQUEST_PARAMS, " Failed send To Device Rpc Request: " +
                            rpcRequest.getMethodName() + ". " + e.getMessage());
        }
    }

}
