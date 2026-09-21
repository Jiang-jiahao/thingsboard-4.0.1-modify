package com.jnks.iot.server.queue.discovery;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
public class TopicService {

    @Value("${queue.prefix:}")
    private String prefix;

    @Value("${queue.core.notifications-topic:jnks_iot_core.notifications}")
    private String jnksIotCoreNotificationsTopic;

    @Value("${queue.rule-engine.notifications-topic:jnks_iot_rule_engine.notifications}")
    private String jnksIotRuleEngineNotificationsTopic;

    @Value("${queue.transport.notifications-topic:jnks_iot_transport.notifications}")
    private String jnksIotTransportNotificationsTopic;

    @Value("${queue.calculated-fields.notifications-topic:calculated_field.notifications}")
    private String jnksIotCalculatedFieldNotificationsTopic;

    private final ConcurrentMap<String, TopicPartitionInfo> jnksIotCoreNotificationTopics = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, TopicPartitionInfo> jnksIotRuleEngineNotificationTopics = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, TopicPartitionInfo> jnksIotCalculatedFieldNotificationTopics = new ConcurrentHashMap<>();

    /**
     * Each Service should start a consumer for messages that target individual service instance based on serviceId.
     * This topic is likely to have single partition, and is always assigned to the service.
     * @param serviceType
     * @param serviceId
     * @return
     */
    public TopicPartitionInfo getNotificationsTopic(ServiceType serviceType, String serviceId) {
        return switch (serviceType) {
            case JNKS_IOT_CORE -> jnksIotCoreNotificationTopics.computeIfAbsent(serviceId,
                    id -> buildNotificationsTopicPartitionInfo(jnksIotCoreNotificationsTopic, serviceId));
            case JNKS_IOT_RULE_ENGINE -> jnksIotRuleEngineNotificationTopics.computeIfAbsent(serviceId,
                    id -> buildNotificationsTopicPartitionInfo(jnksIotRuleEngineNotificationsTopic, serviceId));
            case JNKS_IOT_TRANSPORT -> buildNotificationsTopicPartitionInfo(jnksIotTransportNotificationsTopic, serviceId);
            default -> throw new IllegalStateException("Unexpected service type: " + serviceType);
        };
    }

    private TopicPartitionInfo buildNotificationsTopicPartitionInfo(String topic, String serviceId) {
        return buildTopicPartitionInfo(buildNotificationTopicName(topic, serviceId), null, null, false);
    }

    public TopicPartitionInfo buildTopicPartitionInfo(String topic, TenantId tenantId, Integer partition, boolean myPartition) {
        return new TopicPartitionInfo(buildTopicName(topic), tenantId, partition, myPartition);
    }

    public TopicPartitionInfo getCalculatedFieldNotificationsTopic(String serviceId) {
        return jnksIotCalculatedFieldNotificationTopics.computeIfAbsent(serviceId, id -> buildNotificationsTopicPartitionInfo(jnksIotCalculatedFieldNotificationsTopic, serviceId));
    }

    public String buildTopicName(String topic) {
        if (topic == null) {
            return null;
        }
        return prefix.isBlank() ? topic : prefix + "." + topic;
    }

    private String buildNotificationTopicName(String topic, String serviceId) {
        return topic + "." + serviceId;
    }

    public String buildConsumerGroupId(String servicePrefix, TenantId tenantId, String queueName, Integer partitionId) {
        return this.buildTopicName(
                servicePrefix + queueName
                        + (tenantId.isSysTenantId() ? "" : ("-isolated-" + tenantId))
                        + "-consumer"
                        + suffix(partitionId));
    }

    String suffix(Integer partitionId) {
        return partitionId == null ? "" : "-" + partitionId;
    }

}
