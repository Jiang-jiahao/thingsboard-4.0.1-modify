package com.jnks.iot.server.queue.provider;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCoreMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCoreNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToHousekeeperServiceMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToRuleEngineMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToTransportMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToUsageStatsServiceMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.TransportApiRequestMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.TransportApiResponseMsg;
import com.jnks.iot.server.queue.JnksIotQueueAdmin;
import com.jnks.iot.server.queue.JnksIotQueueConsumer;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.JnksIotQueueRequestTemplate;
import com.jnks.iot.server.queue.common.DefaultJnksIotQueueRequestTemplate;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.queue.discovery.JnksIotServiceInfoProvider;
import com.jnks.iot.server.queue.discovery.TopicService;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaAdmin;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaConsumerStatsService;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaConsumerTemplate;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaProducerTemplate;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaSettings;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaTopicConfigs;
import com.jnks.iot.server.queue.settings.JnksIotQueueTransportApiSettings;
import com.jnks.iot.server.queue.settings.JnksIotQueueTransportNotificationSettings;

@Component
@ConditionalOnExpression("'${queue.type:null}'=='kafka' && '${transport.api_enabled:true}'=='true'")
@Slf4j
public class KafkaJnksIotTransportQueueFactory implements JnksIotTransportQueueFactory {

    private final JnksIotKafkaSettings kafkaSettings;
    private final JnksIotServiceInfoProvider serviceInfoProvider;
    private final String coreTopic;
    private final String coreUsageStatsTopic;
    private final String coreHousekeeperTopic;
    private final String ruleEngineTopic;
    private final JnksIotQueueTransportApiSettings transportApiSettings;
    private final JnksIotQueueTransportNotificationSettings transportNotificationSettings;
    private final JnksIotKafkaConsumerStatsService consumerStatsService;
    private final TopicService topicService;

    private final JnksIotQueueAdmin coreAdmin;
    private final JnksIotQueueAdmin ruleEngineAdmin;
    private final JnksIotQueueAdmin transportApiRequestAdmin;
    private final JnksIotQueueAdmin transportApiResponseAdmin;
    private final JnksIotQueueAdmin notificationAdmin;
    private final JnksIotQueueAdmin housekeeperAdmin;

    public KafkaJnksIotTransportQueueFactory(JnksIotKafkaSettings kafkaSettings,
                                        JnksIotServiceInfoProvider serviceInfoProvider,
                                        @Value("${queue.core.topic}") String coreTopic,
                                        @Value("${queue.core.usage-stats-topic:jnks_iot_usage_stats}") String coreUsageStatsTopic,
                                        @Value("${queue.core.housekeeper.topic:jnks_iot_housekeeper}") String coreHousekeeperTopic,
                                        @Value("${queue.rule-engine.topic}") String ruleEngineTopic,
                                        JnksIotQueueTransportApiSettings transportApiSettings,
                                        JnksIotQueueTransportNotificationSettings transportNotificationSettings,
                                        JnksIotKafkaConsumerStatsService consumerStatsService,
                                        JnksIotKafkaTopicConfigs kafkaTopicConfigs,
                                        TopicService topicService) {
        this.kafkaSettings = kafkaSettings;
        this.serviceInfoProvider = serviceInfoProvider;
        this.coreTopic = coreTopic;
        this.coreUsageStatsTopic = coreUsageStatsTopic;
        this.coreHousekeeperTopic = coreHousekeeperTopic;
        this.ruleEngineTopic = ruleEngineTopic;
        this.transportApiSettings = transportApiSettings;
        this.transportNotificationSettings = transportNotificationSettings;
        this.consumerStatsService = consumerStatsService;
        this.topicService = topicService;

        this.coreAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getCoreConfigs());
        this.ruleEngineAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getRuleEngineConfigs());
        this.transportApiRequestAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getTransportApiRequestConfigs());
        this.transportApiResponseAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getTransportApiResponseConfigs());
        this.notificationAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getNotificationsConfigs());
        this.housekeeperAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getHousekeeperConfigs());
    }

    @Override
    public JnksIotQueueRequestTemplate<JnksIotProtoQueueMsg<TransportApiRequestMsg>, JnksIotProtoQueueMsg<TransportApiResponseMsg>> createTransportApiRequestTemplate() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<TransportApiRequestMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("transport-api-request-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(transportApiSettings.getRequestsTopic()));
        requestBuilder.admin(transportApiRequestAdmin);

        JnksIotKafkaConsumerTemplate.JnksIotKafkaConsumerTemplateBuilder<JnksIotProtoQueueMsg<TransportApiResponseMsg>> responseBuilder = JnksIotKafkaConsumerTemplate.builder();
        responseBuilder.settings(kafkaSettings);
        responseBuilder.topic(topicService.buildTopicName(transportApiSettings.getResponsesTopic() + "." + serviceInfoProvider.getServiceId()));
        responseBuilder.clientId("transport-api-response-" + serviceInfoProvider.getServiceId());
        responseBuilder.groupId(topicService.buildTopicName("transport-node-" + serviceInfoProvider.getServiceId()));
        responseBuilder.decoder(msg -> new JnksIotProtoQueueMsg<>(msg.getKey(), TransportApiResponseMsg.parseFrom(msg.getData()), msg.getHeaders()));
        responseBuilder.admin(transportApiResponseAdmin);
        responseBuilder.statsService(consumerStatsService);

        DefaultJnksIotQueueRequestTemplate.DefaultJnksIotQueueRequestTemplateBuilder
                <JnksIotProtoQueueMsg<TransportApiRequestMsg>, JnksIotProtoQueueMsg<TransportApiResponseMsg>> templateBuilder = DefaultJnksIotQueueRequestTemplate.builder();
        templateBuilder.queueAdmin(transportApiResponseAdmin);
        templateBuilder.requestTemplate(requestBuilder.build());
        templateBuilder.responseTemplate(responseBuilder.build());
        templateBuilder.maxPendingRequests(transportApiSettings.getMaxPendingRequests());
        templateBuilder.maxRequestTimeout(transportApiSettings.getMaxRequestsTimeout());
        templateBuilder.pollInterval(transportApiSettings.getResponsePollInterval());
        return templateBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToRuleEngineMsg>> createRuleEngineMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToRuleEngineMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("transport-node-rule-engine-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(ruleEngineTopic));
        requestBuilder.admin(ruleEngineAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreMsg>> createJnksIotCoreMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToCoreMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("transport-node-core-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(coreTopic));
        requestBuilder.admin(coreAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> createJnksIotCoreNotificationsMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("transport-node-to-core-notifications-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(coreTopic));
        requestBuilder.admin(notificationAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToTransportMsg>> createTransportNotificationsConsumer() {
        JnksIotKafkaConsumerTemplate.JnksIotKafkaConsumerTemplateBuilder<JnksIotProtoQueueMsg<ToTransportMsg>> responseBuilder = JnksIotKafkaConsumerTemplate.builder();
        responseBuilder.settings(kafkaSettings);
        responseBuilder.topic(topicService.buildTopicName(transportNotificationSettings.getNotificationsTopic() + "." + serviceInfoProvider.getServiceId()));
        responseBuilder.clientId("transport-api-notifications-" + serviceInfoProvider.getServiceId());
        responseBuilder.groupId(topicService.buildTopicName("transport-node-" + serviceInfoProvider.getServiceId()));
        responseBuilder.decoder(msg -> new JnksIotProtoQueueMsg<>(msg.getKey(), ToTransportMsg.parseFrom(msg.getData()), msg.getHeaders()));
        responseBuilder.admin(notificationAdmin);
        responseBuilder.statsService(consumerStatsService);
        return responseBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToUsageStatsServiceMsg>> createToUsageStatsServiceMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToUsageStatsServiceMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("transport-node-us-producer-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(coreUsageStatsTopic));
        requestBuilder.admin(coreAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> createHousekeeperMsgProducer() {
        return JnksIotKafkaProducerTemplate.<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>>builder()
                .settings(kafkaSettings)
                .clientId("jnks-iot-transport-housekeeper-producer-" + serviceInfoProvider.getServiceId())
                .defaultTopic(topicService.buildTopicName(coreHousekeeperTopic))
                .admin(housekeeperAdmin)
                .build();
    }

    @PreDestroy
    private void destroy() {
        if (coreAdmin != null) {
            coreAdmin.destroy();
        }
        if (ruleEngineAdmin != null) {
            ruleEngineAdmin.destroy();
        }
        if (transportApiRequestAdmin != null) {
            transportApiRequestAdmin.destroy();
        }
        if (transportApiResponseAdmin != null) {
            transportApiResponseAdmin.destroy();
        }
        if (notificationAdmin != null) {
            notificationAdmin.destroy();
        }
    }
}
