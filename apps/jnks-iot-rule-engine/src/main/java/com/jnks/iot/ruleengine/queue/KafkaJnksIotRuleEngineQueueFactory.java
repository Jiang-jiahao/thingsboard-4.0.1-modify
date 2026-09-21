package com.jnks.iot.ruleengine.queue;

import com.jnks.iot.server.queue.provider.*;

import com.google.protobuf.util.JsonFormat;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.DataConstants;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.queue.Queue;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.gen.js.JsInvokeProtos;
import com.jnks.iot.server.gen.transport.TransportProtos.CalculatedFieldStateProto;
import com.jnks.iot.server.gen.transport.TransportProtos.FromEdqsMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCalculatedFieldMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCalculatedFieldNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCoreMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCoreNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToEdqsMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToHousekeeperServiceMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToOtaPackageStateServiceMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToRuleEngineMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToRuleEngineNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToTransportMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToUsageStatsServiceMsg;
import com.jnks.iot.server.queue.JnksIotQueueAdmin;
import com.jnks.iot.server.queue.JnksIotQueueConsumer;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.JnksIotQueueRequestTemplate;
import com.jnks.iot.server.queue.common.DefaultJnksIotQueueRequestTemplate;
import com.jnks.iot.server.queue.common.JnksIotProtoJsQueueMsg;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.queue.discovery.JnksIotServiceInfoProvider;
import com.jnks.iot.server.queue.discovery.TopicService;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaAdmin;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaConsumerStatsService;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaConsumerTemplate;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaProducerTemplate;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaSettings;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaTopicConfigs;
import com.jnks.iot.server.queue.settings.JnksIotQueueCalculatedFieldSettings;
import com.jnks.iot.server.queue.settings.JnksIotQueueRemoteJsInvokeSettings;
import com.jnks.iot.server.queue.settings.JnksIotQueueRuleEngineSettings;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class KafkaJnksIotRuleEngineQueueFactory implements JnksIotRuleEngineQueueFactory {

    private final TopicService topicService;
    private final JnksIotKafkaSettings kafkaSettings;
    private final JnksIotServiceInfoProvider serviceInfoProvider;
    private final String coreTopic;
    private final String coreOtaPackageTopic;
    private final String coreUsageStatsTopic;
    private final String coreHousekeeperTopic;
    private final JnksIotQueueRuleEngineSettings ruleEngineSettings;
    private final JnksIotQueueRemoteJsInvokeSettings jsInvokeSettings;
    private final JnksIotKafkaConsumerStatsService consumerStatsService;
    private final String transportNotificationsTopic;
    private final JnksIotQueueCalculatedFieldSettings calculatedFieldSettings;
    private final String edqsEventsTopic;

    private final JnksIotQueueAdmin coreAdmin;
    private final JnksIotKafkaAdmin ruleEngineAdmin;
    private final JnksIotQueueAdmin jsExecutorRequestAdmin;
    private final JnksIotQueueAdmin jsExecutorResponseAdmin;
    private final JnksIotQueueAdmin notificationAdmin;
    private final JnksIotQueueAdmin fwUpdatesAdmin;
    private final JnksIotQueueAdmin housekeeperAdmin;
    private final JnksIotQueueAdmin cfAdmin;
    private final JnksIotQueueAdmin cfStateAdmin;
    private final JnksIotQueueAdmin edqsEventsAdmin;
    private final AtomicLong consumerCount = new AtomicLong();

    public KafkaJnksIotRuleEngineQueueFactory(TopicService topicService, JnksIotKafkaSettings kafkaSettings,
                                         JnksIotServiceInfoProvider serviceInfoProvider,
                                         @Value("${queue.core.topic}") String coreTopic,
                                         @Value("${queue.core.ota.topic:jnks_iot_ota_package}") String coreOtaPackageTopic,
                                         @Value("${queue.core.usage-stats-topic:jnks_iot_usage_stats}") String coreUsageStatsTopic,
                                         @Value("${queue.core.housekeeper.topic:jnks_iot_housekeeper}") String coreHousekeeperTopic,
                                         JnksIotQueueRuleEngineSettings ruleEngineSettings,
                                         JnksIotQueueRemoteJsInvokeSettings jsInvokeSettings,
                                         JnksIotKafkaConsumerStatsService consumerStatsService,
                                         @Value("${queue.transport.notifications_topic}") String transportNotificationsTopic,
                                         JnksIotQueueCalculatedFieldSettings calculatedFieldSettings,
                                         @Value("${queue.edqs.events_topic:edqs.events}") String edqsEventsTopic,
                                         JnksIotKafkaTopicConfigs kafkaTopicConfigs) {
        this.topicService = topicService;
        this.kafkaSettings = kafkaSettings;
        this.serviceInfoProvider = serviceInfoProvider;
        this.coreTopic = coreTopic;
        this.coreOtaPackageTopic = coreOtaPackageTopic;
        this.coreUsageStatsTopic = coreUsageStatsTopic;
        this.coreHousekeeperTopic = coreHousekeeperTopic;
        this.ruleEngineSettings = ruleEngineSettings;
        this.jsInvokeSettings = jsInvokeSettings;
        this.consumerStatsService = consumerStatsService;
        this.transportNotificationsTopic = transportNotificationsTopic;
        this.calculatedFieldSettings = calculatedFieldSettings;
        this.edqsEventsTopic = edqsEventsTopic;

        this.coreAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getCoreConfigs());
        this.ruleEngineAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getRuleEngineConfigs());
        this.jsExecutorRequestAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getJsExecutorRequestConfigs());
        this.jsExecutorResponseAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getJsExecutorResponseConfigs());
        this.notificationAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getNotificationsConfigs());
        this.fwUpdatesAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getFwUpdatesConfigs());
        this.housekeeperAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getHousekeeperConfigs());
        this.cfAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getCalculatedFieldConfigs());
        this.cfStateAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getCalculatedFieldStateConfigs());
        this.edqsEventsAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getEdqsEventsConfigs());
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToTransportMsg>> createTransportNotificationsMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToTransportMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("jnks-iot-rule-engine-transport-notifications-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(transportNotificationsTopic));
        requestBuilder.admin(notificationAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToRuleEngineMsg>> createRuleEngineMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToRuleEngineMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("jnks-iot-rule-engine-to-rule-engine-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(ruleEngineSettings.getTopic()));
        requestBuilder.admin(ruleEngineAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToRuleEngineNotificationMsg>> createRuleEngineNotificationsMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToRuleEngineNotificationMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("jnks-iot-rule-engine-to-rule-engine-notifications-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(ruleEngineSettings.getTopic()));
        requestBuilder.admin(notificationAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreMsg>> createJnksIotCoreMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToCoreMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("jnks-iot-rule-engine-to-core-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(coreTopic));
        requestBuilder.admin(coreAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToOtaPackageStateServiceMsg>> createToOtaPackageStateServiceMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToOtaPackageStateServiceMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("jnks-iot-rule-engine-ota-producer-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(coreOtaPackageTopic));
        requestBuilder.admin(fwUpdatesAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> createJnksIotCoreNotificationsMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("jnks-iot-rule-engine-to-core-notifications-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(coreTopic));
        requestBuilder.admin(notificationAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToRuleEngineMsg>> createToRuleEngineMsgConsumer(Queue configuration) {
        throw new UnsupportedOperationException("Rule engine consumer should use a partitionId");
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToRuleEngineMsg>> createToRuleEngineMsgConsumer(Queue configuration, Integer partitionId) {
        String queueName = configuration.getName();
        String groupId = topicService.buildConsumerGroupId("re-", configuration.getTenantId(), queueName, partitionId);

        ruleEngineAdmin.syncOffsets(topicService.buildConsumerGroupId("re-", configuration.getTenantId(), queueName, null), // the fat groupId
                groupId, partitionId);

        JnksIotKafkaConsumerTemplate.JnksIotKafkaConsumerTemplateBuilder<JnksIotProtoQueueMsg<ToRuleEngineMsg>> consumerBuilder = JnksIotKafkaConsumerTemplate.builder();
        consumerBuilder.settings(kafkaSettings);
        consumerBuilder.topic(topicService.buildTopicName(configuration.getTopic()));
        consumerBuilder.clientId("re-" + queueName + "-consumer-" + serviceInfoProvider.getServiceId() + "-" + consumerCount.incrementAndGet());
        consumerBuilder.groupId(groupId);
        consumerBuilder.decoder(msg -> new JnksIotProtoQueueMsg<>(msg.getKey(), ToRuleEngineMsg.parseFrom(msg.getData()), msg.getHeaders()));
        consumerBuilder.admin(ruleEngineAdmin);
        consumerBuilder.statsService(consumerStatsService);
        return consumerBuilder.build();
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToRuleEngineNotificationMsg>> createToRuleEngineNotificationsMsgConsumer() {
        JnksIotKafkaConsumerTemplate.JnksIotKafkaConsumerTemplateBuilder<JnksIotProtoQueueMsg<ToRuleEngineNotificationMsg>> consumerBuilder = JnksIotKafkaConsumerTemplate.builder();
        consumerBuilder.settings(kafkaSettings);
        consumerBuilder.topic(topicService.getNotificationsTopic(ServiceType.JNKS_IOT_RULE_ENGINE, serviceInfoProvider.getServiceId()).getFullTopicName());
        consumerBuilder.clientId("jnks-iot-rule-engine-notifications-consumer-" + serviceInfoProvider.getServiceId());
        consumerBuilder.groupId(topicService.buildTopicName("jnks-iot-rule-engine-notifications-node-") + serviceInfoProvider.getServiceId());
        consumerBuilder.decoder(msg -> new JnksIotProtoQueueMsg<>(msg.getKey(), ToRuleEngineNotificationMsg.parseFrom(msg.getData()), msg.getHeaders()));
        consumerBuilder.admin(notificationAdmin);
        consumerBuilder.statsService(consumerStatsService);
        return consumerBuilder.build();
    }

    @Override
    @Bean
    public JnksIotQueueRequestTemplate<JnksIotProtoJsQueueMsg<JsInvokeProtos.RemoteJsRequest>, JnksIotProtoQueueMsg<JsInvokeProtos.RemoteJsResponse>> createRemoteJsRequestTemplate() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoJsQueueMsg<JsInvokeProtos.RemoteJsRequest>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("producer-js-invoke-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(jsInvokeSettings.getRequestTopic());
        requestBuilder.admin(jsExecutorRequestAdmin);

        JnksIotKafkaConsumerTemplate.JnksIotKafkaConsumerTemplateBuilder<JnksIotProtoQueueMsg<JsInvokeProtos.RemoteJsResponse>> responseBuilder = JnksIotKafkaConsumerTemplate.builder();
        responseBuilder.settings(kafkaSettings);
        responseBuilder.topic(jsInvokeSettings.getResponseTopic() + "." + serviceInfoProvider.getServiceId());
        responseBuilder.clientId("js-" + serviceInfoProvider.getServiceId());
        responseBuilder.groupId(topicService.buildTopicName("rule-engine-node-") + serviceInfoProvider.getServiceId());
        responseBuilder.decoder(msg -> {
                    JsInvokeProtos.RemoteJsResponse.Builder builder = JsInvokeProtos.RemoteJsResponse.newBuilder();
                    JsonFormat.parser().ignoringUnknownFields().merge(new String(msg.getData(), StandardCharsets.UTF_8), builder);
                    return new JnksIotProtoQueueMsg<>(msg.getKey(), builder.build(), msg.getHeaders());
                }
        );
        responseBuilder.admin(jsExecutorResponseAdmin);
        responseBuilder.statsService(consumerStatsService);

        DefaultJnksIotQueueRequestTemplate.DefaultJnksIotQueueRequestTemplateBuilder
                <JnksIotProtoJsQueueMsg<JsInvokeProtos.RemoteJsRequest>, JnksIotProtoQueueMsg<JsInvokeProtos.RemoteJsResponse>> builder = DefaultJnksIotQueueRequestTemplate.builder();
        builder.queueAdmin(jsExecutorResponseAdmin);
        builder.requestTemplate(requestBuilder.build());
        builder.responseTemplate(responseBuilder.build());
        builder.maxPendingRequests(jsInvokeSettings.getMaxPendingRequests());
        builder.maxRequestTimeout(jsInvokeSettings.getMaxRequestsTimeout());
        builder.pollInterval(jsInvokeSettings.getResponsePollInterval());
        return builder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToUsageStatsServiceMsg>> createToUsageStatsServiceMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToUsageStatsServiceMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("jnks-iot-rule-engine-us-producer-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(coreUsageStatsTopic));
        requestBuilder.admin(coreAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> createHousekeeperMsgProducer() {
        return JnksIotKafkaProducerTemplate.<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>>builder()
                .settings(kafkaSettings)
                .clientId("jnks-iot-rule-engine-housekeeper-producer-" + serviceInfoProvider.getServiceId())
                .defaultTopic(topicService.buildTopicName(coreHousekeeperTopic))
                .admin(housekeeperAdmin)
                .build();
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToCalculatedFieldMsg>> createToCalculatedFieldMsgConsumer(TopicPartitionInfo tpi) {
        String queueName = DataConstants.CF_QUEUE_NAME;
        if (tpi == null) {
            throw new IllegalArgumentException("TopicPartitionInfo is required.");
        }
        TenantId tenantId = tpi.getTenantId().orElse(TenantId.SYS_TENANT_ID);
        Integer partitionId = tpi.getPartition().orElseThrow(() -> new IllegalArgumentException("PartitionId is required."));
        String groupId = topicService.buildConsumerGroupId("cf-", tenantId, queueName, partitionId);

        JnksIotKafkaConsumerTemplate.JnksIotKafkaConsumerTemplateBuilder<JnksIotProtoQueueMsg<ToCalculatedFieldMsg>> consumerBuilder = JnksIotKafkaConsumerTemplate.builder();
        consumerBuilder.settings(kafkaSettings);
        consumerBuilder.topic(topicService.buildTopicName(calculatedFieldSettings.getEventTopic()));
        consumerBuilder.clientId("cf-" + queueName + "-consumer-" + serviceInfoProvider.getServiceId() + "-" + consumerCount.incrementAndGet());
        consumerBuilder.groupId(groupId);
        consumerBuilder.decoder(msg -> new JnksIotProtoQueueMsg<>(msg.getKey(), ToCalculatedFieldMsg.parseFrom(msg.getData()), msg.getHeaders()));
        consumerBuilder.admin(cfAdmin);
        consumerBuilder.statsService(consumerStatsService);
        return consumerBuilder.build();
    }

    @Override
    public JnksIotQueueAdmin getCalculatedFieldQueueAdmin() {
        return cfAdmin;
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCalculatedFieldMsg>> createToCalculatedFieldMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToCalculatedFieldMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("jnks-iot-rule-engine-to-calculated-field-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(calculatedFieldSettings.getEventTopic()));
        requestBuilder.admin(cfAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToCalculatedFieldNotificationMsg>> createToCalculatedFieldNotificationMsgConsumer() {
        JnksIotKafkaConsumerTemplate.JnksIotKafkaConsumerTemplateBuilder<JnksIotProtoQueueMsg<ToCalculatedFieldNotificationMsg>> consumerBuilder = JnksIotKafkaConsumerTemplate.builder();
        consumerBuilder.settings(kafkaSettings);
        consumerBuilder.topic(topicService.getCalculatedFieldNotificationsTopic(serviceInfoProvider.getServiceId()).getFullTopicName());
        consumerBuilder.clientId("jnks-iot-calculated-field-notifications-consumer-" + serviceInfoProvider.getServiceId());
        consumerBuilder.groupId(topicService.buildTopicName("jnks-iot-calculated-field-notifications-node-") + serviceInfoProvider.getServiceId());
        consumerBuilder.decoder(msg -> new JnksIotProtoQueueMsg<>(msg.getKey(), ToCalculatedFieldNotificationMsg.parseFrom(msg.getData()), msg.getHeaders()));
        consumerBuilder.admin(notificationAdmin);
        consumerBuilder.statsService(consumerStatsService);
        return consumerBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCalculatedFieldNotificationMsg>> createToCalculatedFieldNotificationMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToCalculatedFieldNotificationMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("jnks-iot-calculated-field-notifications-producer-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.getCalculatedFieldNotificationsTopic(serviceInfoProvider.getServiceId()).getFullTopicName());
        requestBuilder.admin(notificationAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<CalculatedFieldStateProto>> createCalculatedFieldStateConsumer() {
        return JnksIotKafkaConsumerTemplate.<JnksIotProtoQueueMsg<CalculatedFieldStateProto>>builder()
                .settings(kafkaSettings)
                .topic(topicService.buildTopicName(calculatedFieldSettings.getStateTopic()))
                .readFromBeginning(true)
                .stopWhenRead(true)
                .clientId("jnks-iot-rule-engine-calculated-field-state-consumer-" + serviceInfoProvider.getServiceId() + "-" + consumerCount.incrementAndGet())
                .groupId(null) // not using consumer group management
                .decoder(msg -> new JnksIotProtoQueueMsg<>(msg.getKey(), msg.getData() != null ? CalculatedFieldStateProto.parseFrom(msg.getData()) : null, msg.getHeaders()))
                .admin(cfStateAdmin)
                .statsService(consumerStatsService)
                .build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<CalculatedFieldStateProto>> createCalculatedFieldStateProducer() {
        return JnksIotKafkaProducerTemplate.<JnksIotProtoQueueMsg<CalculatedFieldStateProto>>builder()
                .settings(kafkaSettings)
                .clientId("jnks-iot-rule-engine-to-calculated-field-state-" + serviceInfoProvider.getServiceId())
                .defaultTopic(topicService.buildTopicName(calculatedFieldSettings.getEventTopic()))
                .admin(cfStateAdmin)
                .build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToEdqsMsg>> createEdqsEventsProducer() {
        return JnksIotKafkaProducerTemplate.<JnksIotProtoQueueMsg<ToEdqsMsg>>builder()
                .clientId("edqs-events-producer-" + serviceInfoProvider.getServiceId())
                .defaultTopic(topicService.buildTopicName(edqsEventsTopic))
                .settings(kafkaSettings)
                .admin(edqsEventsAdmin)
                .build();
    }

    @Override
    public JnksIotQueueRequestTemplate<JnksIotProtoQueueMsg<ToEdqsMsg>, JnksIotProtoQueueMsg<FromEdqsMsg>> createEdqsRequestTemplate() {
        throw new UnsupportedOperationException();
    }

    @PreDestroy
    private void destroy() {
        if (coreAdmin != null) {
            coreAdmin.destroy();
        }
        if (ruleEngineAdmin != null) {
            ruleEngineAdmin.destroy();
        }
        if (jsExecutorRequestAdmin != null) {
            jsExecutorRequestAdmin.destroy();
        }
        if (jsExecutorResponseAdmin != null) {
            jsExecutorResponseAdmin.destroy();
        }
        if (notificationAdmin != null) {
            notificationAdmin.destroy();
        }
        if (fwUpdatesAdmin != null) {
            fwUpdatesAdmin.destroy();
        }
        if (cfAdmin != null) {
            cfAdmin.destroy();
        }
    }
}
