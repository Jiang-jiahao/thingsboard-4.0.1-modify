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

import io.netty.handler.codec.mqtt.MqttQoS;
import lombok.ToString;
import org.thingsboard.server.common.transport.session.DeviceAwareSessionContext;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * MQTT设备感知的会话上下文抽象基类。
 * 核心职责：管理与特定MQTT设备会话相关的状态，特别是维护该设备订阅的各个主题与其对应QoS级别的映射关系。
 * 设备会话上下文在设备连接期间持续存在，用于存储会话状态信息。
 * <p>
 * 设计说明：继承自DeviceAwareSessionContext，从而具备了设备身份识别等基础能力，
 * 并在此基础上添加了MQTT协议特有的QoS映射管理功能。
 * Created by ashvayka on 30.08.18.
 */
@ToString(callSuper = true)
public abstract class MqttDeviceAwareSessionContext extends DeviceAwareSessionContext {

    /**
     * 线程安全的映射表：存储MQTT主题匹配器与对应的QoS等级。
     * - Key: MqttTopicMatcher - 主题匹配器，用于匹配设备订阅的主题（可能包含通配符）
     * - Value: Integer - MQTT QoS等级（0、1、2），表示消息的传递保证级别
     * 使用ConcurrentMap确保多线程环境下的安全访问。
     */
    private final ConcurrentMap<MqttTopicMatcher, Integer> mqttQoSMap;

    /**
     * 构造函数：初始化会话上下文。
     *
     * @param sessionId  此会话的唯一标识符，用于在系统中跟踪和管理会话
     * @param mqttQoSMap 预构建的QoS映射表，包含此设备会话已订阅的所有主题及其QoS设置
     */
    public MqttDeviceAwareSessionContext(UUID sessionId, ConcurrentMap<MqttTopicMatcher, Integer> mqttQoSMap) {
        super(sessionId);
        this.mqttQoSMap = mqttQoSMap;
    }

    /**
     * 获取当前会话的完整QoS映射表。
     * 此映射反映了设备在当前会话中所有活跃的订阅关系。
     *
     * @return 线程安全的QoS映射表，可用于查询或修改订阅状态
     */
    public ConcurrentMap<MqttTopicMatcher, Integer> getMqttQoSMap() {
        return mqttQoSMap;
    }

    /**
     * 根据主题名称查找并返回适用的QoS等级。
     * 该方法遍历所有已注册的主题匹配器，找到第一个匹配给定主题的项，并返回其对应的QoS值。
     * 这是MQTT消息路由中的关键方法，用于确定向特定主题发布消息时应使用的服务质量级别。
     *
     * @param topic 要查找的MQTT主题名称（例如："devices/sensor1/temperature"）
     * @return 匹配主题的QoS等级；若未找到明确匹配，则返回默认的AT_LEAST_ONCE(QoS 1)
     * <p>
     * 匹配逻辑说明：
     * 1. 遍历mqttQoSMap中的所有条目
     * 2. 对每个MqttTopicMatcher检查是否与输入topic匹配
     * 3. 收集所有匹配项的QoS值
     * 4. 返回第一个匹配项的QoS（通常应该有且仅有一个匹配）
     * 5. 若无匹配，则返回默认的"至少一次"保证级别
     */
    public MqttQoS getQoSForTopic(String topic) {
        List<Integer> qosList = mqttQoSMap.entrySet()
                .stream()
                .filter(entry -> entry.getKey().matches(topic))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
        if (!qosList.isEmpty()) {
            return MqttQoS.valueOf(qosList.get(0));
        } else {
            return MqttQoS.AT_LEAST_ONCE;
        }
    }
}
