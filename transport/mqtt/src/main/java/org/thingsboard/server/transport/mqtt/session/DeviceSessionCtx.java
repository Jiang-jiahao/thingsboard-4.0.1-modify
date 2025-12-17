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

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.util.ReferenceCountUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.TransportPayloadType;
import org.thingsboard.server.common.data.device.profile.DeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.MqttDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.ProtoTransportPayloadConfiguration;
import org.thingsboard.server.common.data.device.profile.TransportPayloadTypeConfiguration;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.transport.mqtt.MqttTransportContext;
import org.thingsboard.server.transport.mqtt.TopicType;
import org.thingsboard.server.transport.mqtt.adaptors.BackwardCompatibilityAdaptor;
import org.thingsboard.server.transport.mqtt.adaptors.MqttTransportAdaptor;
import org.thingsboard.server.transport.mqtt.util.MqttTopicFilter;
import org.thingsboard.server.transport.mqtt.util.MqttTopicFilterFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * 设备会话上下文类，用于管理单个MQTT设备连接的状态和配置。（直连设备使用）
 * 继承自MqttDeviceAwareSessionContext，为每个设备连接维护独立的会话状态。
 * <p>
 * 核心职责：
 * 1. 管理设备通道（Channel）和MQTT协议版本
 * 2. 处理消息队列和消息ID序列
 * 3. 管理设备配置（主题过滤、负载类型、协议适配器等）
 * 4. 支持设备配置的动态更新
 *
 * @author Andrew Shvayka
 */
@Slf4j
public class DeviceSessionCtx extends MqttDeviceAwareSessionContext {

    /**
     * Netty通道上下文，用于网络通信
     */
    @Getter
    @Setter
    private ChannelHandlerContext channel;

    /**
     * MQTT传输层的全局上下文
     */
    @Getter
    private final MqttTransportContext context;

    /**
     * 消息ID序列生成器，用于生成唯一的MQTT消息ID（Packet Identifier）
     */
    private final AtomicInteger msgIdSeq = new AtomicInteger(0);

    /**
     * 消息队列，用于临时存储待处理的MQTT消息（支持并发访问）
     */
    private final ConcurrentLinkedQueue<MqttMessage> msgQueue = new ConcurrentLinkedQueue<>();

    /**
     * 消息队列处理锁，保证线程安全
     */
    @Getter
    private final Lock msgQueueProcessorLock = new ReentrantLock();

    /**
     * 消息队列大小计数器（原子操作，确保准确计数）
     */
    private final AtomicInteger msgQueueSize = new AtomicInteger(0);

    /**
     * 标记此会话是否仅用于设备预配置（自动注册）
     */
    @Getter
    @Setter
    private boolean provisionOnly = false;

    /**
     * MQTT协议版本（如3.1、3.1.1、5.0）
     */
    @Getter
    @Setter
    private MqttVersion mqttVersion;

    /**
     * 主题过滤器：用于匹配设备发布/订阅的各种主题
     */
    private volatile MqttTopicFilter telemetryTopicFilter = MqttTopicFilterFactory.getDefaultTelemetryFilter();
    private volatile MqttTopicFilter attributesPublishTopicFilter = MqttTopicFilterFactory.getDefaultAttributesFilter();
    private volatile MqttTopicFilter attributesSubscribeTopicFilter = MqttTopicFilterFactory.getDefaultAttributesFilter();

    /**
     * 负载类型：JSON或PROTOBUF
     */
    @Getter
    private volatile TransportPayloadType payloadType = TransportPayloadType.JSON;

    /**
     * Protocol Buffers动态消息描述符，用于PROTOBUF负载类型的消息解析
     */
    private volatile Descriptors.Descriptor attributesDynamicMessageDescriptor;
    private volatile Descriptors.Descriptor telemetryDynamicMessageDescriptor;
    private volatile Descriptors.Descriptor rpcResponseDynamicMessageDescriptor;
    private volatile DynamicMessage.Builder rpcRequestDynamicMessageBuilder;

    /**
     * 消息适配器，负责编解码
     */
    private volatile MqttTransportAdaptor adaptor;

    /**
     * JSON负载格式兼容性相关标志
     */
    private volatile boolean jsonPayloadFormatCompatibilityEnabled;
    private volatile boolean useJsonPayloadFormatForDefaultDownlinkTopics;

    /**
     *  验证异常时是否发送ACK
     */
    private volatile boolean sendAckOnValidationException;

    /**
     * 设备配置是否指定了MQTT传输类型
     */
    @Getter
    private volatile boolean deviceProfileMqttTransportType;

    /**
     * 预配置消息的负载类型
     */
    @Getter
    @Setter
    private TransportPayloadType provisionPayloadType = payloadType;


    /**
     * 构造函数：初始化设备会话上下文
     *
     * @param sessionId 会话唯一标识符
     * @param mqttQoSMap QoS映射表（主题匹配器 -> QoS级别）
     * @param context MQTT传输层全局上下文
     */
    public DeviceSessionCtx(UUID sessionId, ConcurrentMap<MqttTopicMatcher, Integer> mqttQoSMap, MqttTransportContext context) {
        super(sessionId, mqttQoSMap);
        this.context = context;
        this.adaptor = context.getJsonMqttAdaptor();
    }

    /**
     * 生成下一个消息ID（线程安全）
     *
     * @return 递增的消息ID序列值
     */
    public int nextMsgId() {
        return msgIdSeq.incrementAndGet();
    }

    /**
     * 检查主题是否为遥测数据主题
     *
     * @param topicName 待检查的主题名称
     * @return 如果匹配遥测主题过滤器则返回true
     */
    public boolean isDeviceTelemetryTopic(String topicName) {
        return telemetryTopicFilter.filter(topicName);
    }

    /**
     * 检查主题是否为属性发布主题（设备 -> 平台）
     *
     * @param topicName 待检查的主题名称
     * @return 如果匹配属性发布过滤器则返回true
     */
    public boolean isDeviceAttributesTopic(String topicName) {
        return attributesPublishTopicFilter.filter(topicName);
    }

    /**
     * 检查主题是否为属性订阅主题（平台 -> 设备）
     *
     * @param topicName 待检查的主题名称
     * @return 如果匹配属性订阅过滤器则返回true
     */
    public boolean isDeviceSubscriptionAttributesTopic(String topicName) {
        return attributesSubscribeTopicFilter.filter(topicName);
    }

    /**
     * 获取当前负载适配器
     */
    public MqttTransportAdaptor getPayloadAdaptor() {
        return adaptor;
    }


    /**
     * 检查当前负载类型是否为JSON
     */
    public boolean isJsonPayloadType() {
        return payloadType.equals(TransportPayloadType.JSON);
    }

    /**
     * 检查是否在验证异常时发送ACK
     */
    public boolean isSendAckOnValidationException() {
        return sendAckOnValidationException;
    }

    /**
     * 获取遥测数据的Protocol Buffers描述符
     */
    public Descriptors.Descriptor getTelemetryDynamicMsgDescriptor() {
        return telemetryDynamicMessageDescriptor;
    }

    /**
     * 获取属性数据的Protocol Buffers描述符
     */
    public Descriptors.Descriptor getAttributesDynamicMessageDescriptor() {
        return attributesDynamicMessageDescriptor;
    }

    /**
     * 获取RPC响应的Protocol Buffers描述符
     */
    public Descriptors.Descriptor getRpcResponseDynamicMessageDescriptor() {
        return rpcResponseDynamicMessageDescriptor;
    }

    /**
     * 获取RPC请求的Protocol Buffers消息构建器
     */
    public DynamicMessage.Builder getRpcRequestDynamicMessageBuilder() {
        return rpcRequestDynamicMessageBuilder;
    }

    /**
     * 设置设备配置，并更新会话配置
     *
     * @param deviceProfile 设备配置信息
     */
    @Override
    public void setDeviceProfile(DeviceProfile deviceProfile) {
        super.setDeviceProfile(deviceProfile);
        updateDeviceSessionConfiguration(deviceProfile);
    }

    /**
     * 设备配置更新时的回调方法
     *
     * @param sessionInfo 会话信息
     * @param deviceProfile 新的设备配置
     */
    @Override
    public void onDeviceProfileUpdate(TransportProtos.SessionInfoProto sessionInfo, DeviceProfile deviceProfile) {
        super.onDeviceProfileUpdate(sessionInfo, deviceProfile);
        updateDeviceSessionConfiguration(deviceProfile);
    }

    /**
     * 根据设备配置更新会话配置
     * <p>
     * 根据设备配置中的传输配置，更新主题过滤器、负载类型、协议适配器等。
     * 支持动态更新，设备连接期间配置变更可以实时生效。
     *
     * @param deviceProfile 设备配置
     */
    private void updateDeviceSessionConfiguration(DeviceProfile deviceProfile) {
        DeviceProfileTransportConfiguration transportConfiguration = deviceProfile.getProfileData().getTransportConfiguration();
        // 检查是否为MQTT传输类型
        if (transportConfiguration.getType().equals(DeviceTransportType.MQTT) &&
                transportConfiguration instanceof MqttDeviceProfileTransportConfiguration) {
            MqttDeviceProfileTransportConfiguration mqttConfig = (MqttDeviceProfileTransportConfiguration) transportConfiguration;
            TransportPayloadTypeConfiguration transportPayloadTypeConfiguration = mqttConfig.getTransportPayloadTypeConfiguration();
            payloadType = transportPayloadTypeConfiguration.getTransportPayloadType();
            deviceProfileMqttTransportType = true;
            // 更新主题过滤器
            telemetryTopicFilter = MqttTopicFilterFactory.toFilter(mqttConfig.getDeviceTelemetryTopic());
            attributesPublishTopicFilter = MqttTopicFilterFactory.toFilter(mqttConfig.getDeviceAttributesTopic());
            attributesSubscribeTopicFilter = MqttTopicFilterFactory.toFilter(mqttConfig.getDeviceAttributesSubscribeTopic());
            sendAckOnValidationException = mqttConfig.isSendAckOnValidationException();
            // 如果是PROTOBUF负载类型，需要初始化Protocol Buffers相关配置
            if (TransportPayloadType.PROTOBUF.equals(payloadType)) {
                ProtoTransportPayloadConfiguration protoTransportPayloadConfig = (ProtoTransportPayloadConfiguration) transportPayloadTypeConfiguration;
                updateDynamicMessageDescriptors(protoTransportPayloadConfig);
                jsonPayloadFormatCompatibilityEnabled = protoTransportPayloadConfig.isEnableCompatibilityWithJsonPayloadFormat();
                useJsonPayloadFormatForDefaultDownlinkTopics = jsonPayloadFormatCompatibilityEnabled && protoTransportPayloadConfig.isUseJsonPayloadFormatForDefaultDownlinkTopics();
            }
        } else {
            // 非MQTT传输类型或配置异常时，使用默认配置
            telemetryTopicFilter = MqttTopicFilterFactory.getDefaultTelemetryFilter();
            attributesPublishTopicFilter = MqttTopicFilterFactory.getDefaultAttributesFilter();
            payloadType = TransportPayloadType.JSON;
            deviceProfileMqttTransportType = false;
            sendAckOnValidationException = false;
        }
        // 根据新的配置更新适配器
        updateAdaptor();
    }

    /**
     * 更新Protocol Buffers动态消息描述符
     *
     * @param protoTransportPayloadConfig Protocol Buffers负载配置
     */
    private void updateDynamicMessageDescriptors(ProtoTransportPayloadConfiguration protoTransportPayloadConfig) {
        telemetryDynamicMessageDescriptor = protoTransportPayloadConfig.getTelemetryDynamicMessageDescriptor(protoTransportPayloadConfig.getDeviceTelemetryProtoSchema());
        attributesDynamicMessageDescriptor = protoTransportPayloadConfig.getAttributesDynamicMessageDescriptor(protoTransportPayloadConfig.getDeviceAttributesProtoSchema());
        rpcResponseDynamicMessageDescriptor = protoTransportPayloadConfig.getRpcResponseDynamicMessageDescriptor(protoTransportPayloadConfig.getDeviceRpcResponseProtoSchema());
        rpcRequestDynamicMessageBuilder = protoTransportPayloadConfig.getRpcRequestDynamicMessageBuilder(protoTransportPayloadConfig.getDeviceRpcRequestProtoSchema());
    }

    /**
     * 根据主题类型获取对应的适配器
     *
     * @param topicType 主题类型（V2、V2_JSON、V2_PROTO等）
     * @return 对应的消息适配器
     */
    public MqttTransportAdaptor getAdaptor(TopicType topicType) {
        switch (topicType) {
            case V2:
                return getDefaultAdaptor();
            case V2_JSON:
                return context.getJsonMqttAdaptor();
            case V2_PROTO:
                return context.getProtoMqttAdaptor();
            default:
                // 默认情况下，如果启用了JSON负载格式且配置了下行主题使用JSON，则使用JSON适配器
                return useJsonPayloadFormatForDefaultDownlinkTopics ? context.getJsonMqttAdaptor() : getDefaultAdaptor();
        }
    }

    /**
     * 获取默认适配器（根据当前负载类型决定）
     */
    private MqttTransportAdaptor getDefaultAdaptor() {
        return isJsonPayloadType() ? context.getJsonMqttAdaptor() : context.getProtoMqttAdaptor();
    }

    /**
     * 根据当前配置更新适配器
     */
    private void updateAdaptor() {
        if (isJsonPayloadType()) {
            adaptor = context.getJsonMqttAdaptor();
            jsonPayloadFormatCompatibilityEnabled = false;
            useJsonPayloadFormatForDefaultDownlinkTopics = false;
        } else {
            // PROTOBUF负载类型
            if (jsonPayloadFormatCompatibilityEnabled) {
                // 启用JSON兼容模式时，使用向后兼容适配器
                adaptor = new BackwardCompatibilityAdaptor(context.getProtoMqttAdaptor(), context.getJsonMqttAdaptor());
            } else {
                adaptor = context.getProtoMqttAdaptor();
            }
        }
    }

    /**
     * 将消息添加到队列（线程安全）
     *
     * @param msg 要添加的MQTT消息
     */
    public void addToQueue(MqttMessage msg) {
        // 增加队列大小计数
        msgQueueSize.incrementAndGet();
        // 增加Netty缓冲区的引用计数，防止被提前释放
        ReferenceCountUtil.retain(msg);
        // 将消息加入队列
        msgQueue.add(msg);
    }

    /**
     * 尝试处理队列中的消息
     *
     * 使用非阻塞锁尝试获取处理权，避免多个线程同时处理队列。
     * 获取锁成功后，顺序处理队列中的所有消息。
     *
     * @param msgProcessor 消息处理器回调函数
     */
    public void tryProcessQueuedMsgs(Consumer<MqttMessage> msgProcessor) {
        while (!msgQueue.isEmpty()) {
            if (msgQueueProcessorLock.tryLock()) {
                try {
                    MqttMessage msg;
                    // 循环处理队列中的所有消息
                    while ((msg = msgQueue.poll()) != null) {
                        try {
                            // 减少队列大小计数
                            msgQueueSize.decrementAndGet();
                            // 调用消息处理器处理消息
                            msgProcessor.accept(msg);
                        } finally {
                            // 确保消息资源被释放
                            ReferenceCountUtil.safeRelease(msg);
                        }
                    }
                } finally {
                    msgQueueProcessorLock.unlock();
                }
            } else {
                // 未能获取锁，说明其他线程正在处理队列，直接返回
                return;
            }
        }
    }

    /**
     * 获取当前消息队列大小
     */
    public int getMsgQueueSize() {
        return msgQueueSize.get();
    }

    /**
     * 释放会话资源
     *
     * 当设备断开连接时调用，清理消息队列中未处理的消息。
     * 如果队列中还有未处理的消息，会记录警告日志并释放这些消息的资源。
     */
    public void release() {
        if (!msgQueue.isEmpty()) {
            log.warn("doDisconnect for device {} but unprocessed messages {} left in the msg queue", getDeviceId(), msgQueue.size());
            msgQueue.forEach(ReferenceCountUtil::safeRelease);
            msgQueue.clear();
        }
    }

    /**
     * 获取消息队列的快照（只读视图）
     *
     * @return 消息队列的不可修改视图
     */
    public Collection<MqttMessage> getMsgQueueSnapshot(){
        return Collections.unmodifiableCollection(msgQueue);
    }

}
