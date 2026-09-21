package com.jnks.iot.monolith.queue;

import com.jnks.iot.server.queue.provider.*;

import com.google.protobuf.util.JsonFormat;
import jakarta.annotation.PreDestroy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
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
import com.jnks.iot.server.gen.transport.TransportProtos.ToVersionControlServiceMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.TransportApiRequestMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.TransportApiResponseMsg;
import com.jnks.iot.server.queue.JnksIotQueueAdmin;
import com.jnks.iot.server.queue.JnksIotQueueConsumer;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.JnksIotQueueRequestTemplate;
import com.jnks.iot.server.queue.common.DefaultJnksIotQueueRequestTemplate;
import com.jnks.iot.server.queue.common.JnksIotProtoJsQueueMsg;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.queue.discovery.JnksIotServiceInfoProvider;
import com.jnks.iot.server.queue.discovery.TopicService;
import com.jnks.iot.server.queue.edqs.EdqsConfig;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaAdmin;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaConsumerStatsService;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaConsumerTemplate;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaProducerTemplate;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaSettings;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaTopicConfigs;
import com.jnks.iot.server.queue.settings.JnksIotQueueCalculatedFieldSettings;
import com.jnks.iot.server.queue.settings.JnksIotQueueCoreSettings;
import com.jnks.iot.server.queue.settings.JnksIotQueueRemoteJsInvokeSettings;
import com.jnks.iot.server.queue.settings.JnksIotQueueRuleEngineSettings;
import com.jnks.iot.server.queue.settings.JnksIotQueueTransportApiSettings;
import com.jnks.iot.server.queue.settings.JnksIotQueueTransportNotificationSettings;
import com.jnks.iot.server.queue.settings.JnksIotQueueVersionControlSettings;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

// Kafka队列实现，但用于单体部署
@Component
@ConditionalOnExpression("'${queue.type:null}'=='kafka'")
public class KafkaMonolithQueueFactory implements JnksIotCoreQueueFactory, JnksIotRuleEngineQueueFactory, JnksIotVersionControlQueueFactory {

    private final TopicService topicService;
    private final JnksIotKafkaSettings kafkaSettings;
    private final JnksIotServiceInfoProvider serviceInfoProvider;
    private final JnksIotQueueCoreSettings coreSettings;
    private final JnksIotQueueRuleEngineSettings ruleEngineSettings;
    private final JnksIotQueueTransportApiSettings transportApiSettings;
    private final JnksIotQueueTransportNotificationSettings transportNotificationSettings;
    private final JnksIotQueueRemoteJsInvokeSettings jsInvokeSettings;
    private final JnksIotQueueVersionControlSettings vcSettings;
    private final JnksIotQueueCalculatedFieldSettings calculatedFieldSettings;
    private final JnksIotKafkaConsumerStatsService consumerStatsService;
    private final EdqsConfig edqsConfig;

    private final JnksIotQueueAdmin coreAdmin;
    private final JnksIotKafkaAdmin ruleEngineAdmin;
    private final JnksIotQueueAdmin jsExecutorRequestAdmin;
    private final JnksIotQueueAdmin jsExecutorResponseAdmin;
    private final JnksIotQueueAdmin transportApiRequestAdmin;
    private final JnksIotQueueAdmin transportApiResponseAdmin;
    private final JnksIotQueueAdmin notificationAdmin;
    private final JnksIotQueueAdmin fwUpdatesAdmin;
    private final JnksIotQueueAdmin vcAdmin;
    private final JnksIotQueueAdmin housekeeperAdmin;
    private final JnksIotQueueAdmin housekeeperReprocessingAdmin;
    private final JnksIotQueueAdmin cfAdmin;
    private final JnksIotQueueAdmin cfStateAdmin;
    private final JnksIotQueueAdmin edqsEventsAdmin;
    private final JnksIotKafkaAdmin edqsRequestsAdmin;

    private final AtomicLong consumerCount = new AtomicLong();

    public KafkaMonolithQueueFactory(TopicService topicService, JnksIotKafkaSettings kafkaSettings,
                                     JnksIotServiceInfoProvider serviceInfoProvider,
                                     JnksIotQueueCoreSettings coreSettings,
                                     JnksIotQueueRuleEngineSettings ruleEngineSettings,
                                     JnksIotQueueTransportApiSettings transportApiSettings,
                                     JnksIotQueueTransportNotificationSettings transportNotificationSettings,
                                     JnksIotQueueRemoteJsInvokeSettings jsInvokeSettings,
                                     JnksIotQueueVersionControlSettings vcSettings,
                                     JnksIotQueueCalculatedFieldSettings calculatedFieldSettings,
                                     JnksIotKafkaConsumerStatsService consumerStatsService,
                                     JnksIotKafkaTopicConfigs kafkaTopicConfigs,
                                     EdqsConfig edqsConfig) {
        this.topicService = topicService;
        this.kafkaSettings = kafkaSettings;
        this.serviceInfoProvider = serviceInfoProvider;
        this.coreSettings = coreSettings;
        this.ruleEngineSettings = ruleEngineSettings;
        this.transportApiSettings = transportApiSettings;
        this.transportNotificationSettings = transportNotificationSettings;
        this.jsInvokeSettings = jsInvokeSettings;
        this.vcSettings = vcSettings;
        this.consumerStatsService = consumerStatsService;
        this.calculatedFieldSettings = calculatedFieldSettings;
        this.edqsConfig = edqsConfig;

        this.coreAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getCoreConfigs());
        this.ruleEngineAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getRuleEngineConfigs());
        this.jsExecutorRequestAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getJsExecutorRequestConfigs());
        this.jsExecutorResponseAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getJsExecutorResponseConfigs());
        this.transportApiRequestAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getTransportApiRequestConfigs());
        this.transportApiResponseAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getTransportApiResponseConfigs());
        this.notificationAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getNotificationsConfigs());
        this.fwUpdatesAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getFwUpdatesConfigs());
        this.vcAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getVcConfigs());
        this.housekeeperAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getHousekeeperConfigs());
        this.housekeeperReprocessingAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getHousekeeperReprocessingConfigs());
        this.cfAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getCalculatedFieldConfigs());
        this.cfStateAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getCalculatedFieldStateConfigs());
        this.edqsEventsAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getEdqsEventsConfigs());
        this.edqsRequestsAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getEdqsRequestsConfigs());
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToTransportMsg>> createTransportNotificationsMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToTransportMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("monolith-transport-notifications-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(transportNotificationSettings.getNotificationsTopic()));
        requestBuilder.admin(notificationAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToRuleEngineMsg>> createRuleEngineMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToRuleEngineMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("monolith-rule-engine-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(ruleEngineSettings.getTopic()));
        requestBuilder.admin(ruleEngineAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToRuleEngineNotificationMsg>> createRuleEngineNotificationsMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToRuleEngineNotificationMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("monolith-rule-engine-notifications-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(ruleEngineSettings.getTopic()));
        requestBuilder.admin(notificationAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreMsg>> createJnksIotCoreMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToCoreMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("monolith-core-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(coreSettings.getTopic()));
        requestBuilder.admin(coreAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> createJnksIotCoreNotificationsMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("monolith-core-notifications-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.getNotificationsTopic(ServiceType.JNKS_IOT_CORE, serviceInfoProvider.getServiceId()).getFullTopicName());
        requestBuilder.admin(notificationAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToVersionControlServiceMsg>> createToVersionControlMsgConsumer() {
        JnksIotKafkaConsumerTemplate.JnksIotKafkaConsumerTemplateBuilder<JnksIotProtoQueueMsg<ToVersionControlServiceMsg>> consumerBuilder = JnksIotKafkaConsumerTemplate.builder();
        consumerBuilder.settings(kafkaSettings);
        consumerBuilder.topic(topicService.buildTopicName(vcSettings.getTopic()));
        consumerBuilder.clientId("monolith-vc-consumer-" + serviceInfoProvider.getServiceId());
        consumerBuilder.groupId(topicService.buildTopicName("monolith-vc-node"));
        consumerBuilder.decoder(msg -> new JnksIotProtoQueueMsg<>(msg.getKey(), ToVersionControlServiceMsg.parseFrom(msg.getData()), msg.getHeaders()));
        consumerBuilder.admin(vcAdmin);
        consumerBuilder.statsService(consumerStatsService);
        return consumerBuilder.build();
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
        consumerBuilder.clientId("monolith-rule-engine-notifications-consumer-" + serviceInfoProvider.getServiceId());
        consumerBuilder.groupId(topicService.buildTopicName("monolith-rule-engine-notifications-consumer-" + serviceInfoProvider.getServiceId()));
        consumerBuilder.decoder(msg -> new JnksIotProtoQueueMsg<>(msg.getKey(), ToRuleEngineNotificationMsg.parseFrom(msg.getData()), msg.getHeaders()));
        consumerBuilder.admin(notificationAdmin);
        consumerBuilder.statsService(consumerStatsService);
        return consumerBuilder.build();
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToCoreMsg>> createToCoreMsgConsumer() {
        JnksIotKafkaConsumerTemplate.JnksIotKafkaConsumerTemplateBuilder<JnksIotProtoQueueMsg<ToCoreMsg>> consumerBuilder = JnksIotKafkaConsumerTemplate.builder();
        consumerBuilder.settings(kafkaSettings);
        consumerBuilder.topic(topicService.buildTopicName(coreSettings.getTopic()));
        consumerBuilder.clientId("monolith-core-consumer-" + serviceInfoProvider.getServiceId() + "-" + consumerCount.incrementAndGet());
        consumerBuilder.groupId(topicService.buildTopicName("monolith-core-consumer"));
        consumerBuilder.decoder(msg -> new JnksIotProtoQueueMsg<>(msg.getKey(), ToCoreMsg.parseFrom(msg.getData()), msg.getHeaders()));
        consumerBuilder.admin(coreAdmin);
        consumerBuilder.statsService(consumerStatsService);
        return consumerBuilder.build();
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> createToCoreNotificationsMsgConsumer() {
        JnksIotKafkaConsumerTemplate.JnksIotKafkaConsumerTemplateBuilder<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> consumerBuilder = JnksIotKafkaConsumerTemplate.builder();
        consumerBuilder.settings(kafkaSettings);
        consumerBuilder.topic(topicService.getNotificationsTopic(ServiceType.JNKS_IOT_CORE, serviceInfoProvider.getServiceId()).getFullTopicName());
        consumerBuilder.clientId("monolith-core-notifications-consumer-" + serviceInfoProvider.getServiceId());
        consumerBuilder.groupId(topicService.buildTopicName("monolith-core-notifications-consumer-" + serviceInfoProvider.getServiceId()));
        consumerBuilder.decoder(msg -> new JnksIotProtoQueueMsg<>(msg.getKey(), ToCoreNotificationMsg.parseFrom(msg.getData()), msg.getHeaders()));
        consumerBuilder.admin(notificationAdmin);
        consumerBuilder.statsService(consumerStatsService);
        return consumerBuilder.build();
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<TransportApiRequestMsg>> createTransportApiRequestConsumer() {
        JnksIotKafkaConsumerTemplate.JnksIotKafkaConsumerTemplateBuilder<JnksIotProtoQueueMsg<TransportApiRequestMsg>> consumerBuilder = JnksIotKafkaConsumerTemplate.builder();
        consumerBuilder.settings(kafkaSettings);
        consumerBuilder.topic(topicService.buildTopicName(transportApiSettings.getRequestsTopic()));
        consumerBuilder.clientId("monolith-transport-api-consumer-" + serviceInfoProvider.getServiceId());
        consumerBuilder.groupId(topicService.buildTopicName("monolith-transport-api-consumer"));
        consumerBuilder.decoder(msg -> new JnksIotProtoQueueMsg<>(msg.getKey(), TransportApiRequestMsg.parseFrom(msg.getData()), msg.getHeaders()));
        consumerBuilder.admin(transportApiRequestAdmin);
        consumerBuilder.statsService(consumerStatsService);
        return consumerBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<TransportApiResponseMsg>> createTransportApiResponseProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<TransportApiResponseMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("monolith-transport-api-producer-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(transportApiSettings.getResponsesTopic()));
        requestBuilder.admin(transportApiResponseAdmin);
        return requestBuilder.build();
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
        responseBuilder.statsService(consumerStatsService);
        responseBuilder.admin(jsExecutorResponseAdmin);

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
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToUsageStatsServiceMsg>> createToUsageStatsServiceMsgConsumer() {
        JnksIotKafkaConsumerTemplate.JnksIotKafkaConsumerTemplateBuilder<JnksIotProtoQueueMsg<ToUsageStatsServiceMsg>> consumerBuilder = JnksIotKafkaConsumerTemplate.builder();
        consumerBuilder.settings(kafkaSettings);
        consumerBuilder.topic(topicService.buildTopicName(coreSettings.getUsageStatsTopic()));
        consumerBuilder.clientId("monolith-us-consumer-" + serviceInfoProvider.getServiceId());
        consumerBuilder.groupId(topicService.buildTopicName("monolith-us-consumer"));
        consumerBuilder.decoder(msg -> new JnksIotProtoQueueMsg<>(msg.getKey(), ToUsageStatsServiceMsg.parseFrom(msg.getData()), msg.getHeaders()));
        consumerBuilder.admin(coreAdmin);
        consumerBuilder.statsService(consumerStatsService);
        return consumerBuilder.build();
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToOtaPackageStateServiceMsg>> createToOtaPackageStateServiceMsgConsumer() {
        JnksIotKafkaConsumerTemplate.JnksIotKafkaConsumerTemplateBuilder<JnksIotProtoQueueMsg<ToOtaPackageStateServiceMsg>> consumerBuilder = JnksIotKafkaConsumerTemplate.builder();
        consumerBuilder.settings(kafkaSettings);
        consumerBuilder.topic(topicService.buildTopicName(coreSettings.getOtaPackageTopic()));
        consumerBuilder.clientId("monolith-ota-consumer-" + serviceInfoProvider.getServiceId());
        consumerBuilder.groupId(topicService.buildTopicName("monolith-ota-consumer"));
        consumerBuilder.decoder(msg -> new JnksIotProtoQueueMsg<>(msg.getKey(), ToOtaPackageStateServiceMsg.parseFrom(msg.getData()), msg.getHeaders()));
        consumerBuilder.admin(fwUpdatesAdmin);
        consumerBuilder.statsService(consumerStatsService);
        return consumerBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToOtaPackageStateServiceMsg>> createToOtaPackageStateServiceMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToOtaPackageStateServiceMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("monolith-ota-producer-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(coreSettings.getOtaPackageTopic()));
        requestBuilder.admin(fwUpdatesAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToUsageStatsServiceMsg>> createToUsageStatsServiceMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToUsageStatsServiceMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("monolith-us-producer-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(coreSettings.getUsageStatsTopic()));
        requestBuilder.admin(coreAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToVersionControlServiceMsg>> createVersionControlMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToVersionControlServiceMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("monolith-vc-producer-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(vcSettings.getTopic()));
        requestBuilder.admin(vcAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> createHousekeeperMsgProducer() {
        return JnksIotKafkaProducerTemplate.<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>>builder()
                .settings(kafkaSettings)
                .clientId("monolith-housekeeper-producer-" + serviceInfoProvider.getServiceId())
                .defaultTopic(topicService.buildTopicName(coreSettings.getHousekeeperTopic()))
                .admin(housekeeperAdmin)
                .build();
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> createHousekeeperMsgConsumer() {
        return JnksIotKafkaConsumerTemplate.<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>>builder()
                .settings(kafkaSettings)
                .topic(topicService.buildTopicName(coreSettings.getHousekeeperTopic()))
                .clientId("monolith-housekeeper-consumer-" + serviceInfoProvider.getServiceId())
                .groupId(topicService.buildTopicName("monolith-housekeeper-consumer"))
                .decoder(msg -> new JnksIotProtoQueueMsg<>(msg.getKey(), ToHousekeeperServiceMsg.parseFrom(msg.getData()), msg.getHeaders()))
                .admin(housekeeperAdmin)
                .statsService(consumerStatsService)
                .build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> createHousekeeperReprocessingMsgProducer() {
        return JnksIotKafkaProducerTemplate.<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>>builder()
                .settings(kafkaSettings)
                .clientId("monolith-housekeeper-reprocessing-producer-" + serviceInfoProvider.getServiceId())
                .defaultTopic(topicService.buildTopicName(coreSettings.getHousekeeperReprocessingTopic()))
                .admin(housekeeperReprocessingAdmin)
                .build();
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> createHousekeeperReprocessingMsgConsumer() {
        return JnksIotKafkaConsumerTemplate.<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>>builder()
                .settings(kafkaSettings)
                .topic(topicService.buildTopicName(coreSettings.getHousekeeperReprocessingTopic()))
                .clientId("monolith-housekeeper-reprocessing-consumer-" + serviceInfoProvider.getServiceId())
                .groupId(topicService.buildTopicName("monolith-housekeeper-reprocessing-consumer"))
                .decoder(msg -> new JnksIotProtoQueueMsg<>(msg.getKey(), ToHousekeeperServiceMsg.parseFrom(msg.getData()), msg.getHeaders()))
                .admin(housekeeperReprocessingAdmin)
                .statsService(consumerStatsService)
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
        requestBuilder.clientId("monolith-calculated-field-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(calculatedFieldSettings.getEventTopic()));
        requestBuilder.admin(cfAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToCalculatedFieldNotificationMsg>> createToCalculatedFieldNotificationMsgConsumer() {
        JnksIotKafkaConsumerTemplate.JnksIotKafkaConsumerTemplateBuilder<JnksIotProtoQueueMsg<ToCalculatedFieldNotificationMsg>> consumerBuilder = JnksIotKafkaConsumerTemplate.builder();
        consumerBuilder.settings(kafkaSettings);
        consumerBuilder.topic(topicService.getCalculatedFieldNotificationsTopic(serviceInfoProvider.getServiceId()).getFullTopicName());
        consumerBuilder.clientId("monolith-calculated-field-notifications-consumer-" + serviceInfoProvider.getServiceId());
        consumerBuilder.groupId(topicService.buildTopicName("monolith-calculated-field-notifications-consumer-" + serviceInfoProvider.getServiceId()));
        consumerBuilder.decoder(msg -> new JnksIotProtoQueueMsg<>(msg.getKey(), ToCalculatedFieldNotificationMsg.parseFrom(msg.getData()), msg.getHeaders()));
        consumerBuilder.admin(notificationAdmin);
        consumerBuilder.statsService(consumerStatsService);
        return consumerBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCalculatedFieldNotificationMsg>> createToCalculatedFieldNotificationMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToCalculatedFieldNotificationMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("monolith-calculated-field-notifications-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(calculatedFieldSettings.getEventTopic()));
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
                .clientId("monolith-calculated-field-state-consumer-" + serviceInfoProvider.getServiceId() + "-" + consumerCount.incrementAndGet())
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
                .clientId("monolith-calculated-field-state-" + serviceInfoProvider.getServiceId())
                .defaultTopic(topicService.buildTopicName(calculatedFieldSettings.getEventTopic()))
                .admin(cfStateAdmin)
                .build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToEdqsMsg>> createEdqsEventsProducer() {
        return JnksIotKafkaProducerTemplate.<JnksIotProtoQueueMsg<ToEdqsMsg>>builder()
                .clientId("edqs-events-producer-" + serviceInfoProvider.getServiceId())
                .defaultTopic(topicService.buildTopicName(edqsConfig.getEventsTopic()))
                .settings(kafkaSettings)
                .admin(edqsEventsAdmin)
                .build();
    }

    @Override
    public JnksIotQueueRequestTemplate<JnksIotProtoQueueMsg<ToEdqsMsg>, JnksIotProtoQueueMsg<FromEdqsMsg>> createEdqsRequestTemplate() {
        var requestProducer = JnksIotKafkaProducerTemplate.<JnksIotProtoQueueMsg<ToEdqsMsg>>builder()
                .settings(kafkaSettings)
                .clientId("edqs-request-" + serviceInfoProvider.getServiceId())
                .defaultTopic(topicService.buildTopicName(edqsConfig.getRequestsTopic()))
                .admin(edqsRequestsAdmin);

        var responseConsumer = JnksIotKafkaConsumerTemplate.<JnksIotProtoQueueMsg<FromEdqsMsg>>builder()
                .settings(kafkaSettings)
                .topic(topicService.buildTopicName(edqsConfig.getResponsesTopic() + "." + serviceInfoProvider.getServiceId()))
                .clientId("monolith-edqs-response-consumer-" + serviceInfoProvider.getServiceId())
                .groupId(topicService.buildTopicName("monolith-edqs-response-consumer-" + serviceInfoProvider.getServiceId()))
                .decoder(msg -> new JnksIotProtoQueueMsg<>(msg.getKey(), FromEdqsMsg.parseFrom(msg.getData()), msg.getHeaders()))
                .admin(edqsRequestsAdmin)
                .statsService(consumerStatsService);

        return DefaultJnksIotQueueRequestTemplate.<JnksIotProtoQueueMsg<ToEdqsMsg>, JnksIotProtoQueueMsg<FromEdqsMsg>>builder()
                .queueAdmin(edqsRequestsAdmin)
                .requestTemplate(requestProducer.build())
                .responseTemplate(responseConsumer.build())
                .maxPendingRequests(edqsConfig.getMaxPendingRequests())
                .maxRequestTimeout(edqsConfig.getMaxRequestTimeout())
                .pollInterval(edqsConfig.getPollInterval())
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
        if (jsExecutorRequestAdmin != null) {
            jsExecutorRequestAdmin.destroy();
        }
        if (jsExecutorResponseAdmin != null) {
            jsExecutorResponseAdmin.destroy();
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
        if (fwUpdatesAdmin != null) {
            fwUpdatesAdmin.destroy();
        }
        if (vcAdmin != null) {
            vcAdmin.destroy();
        }
        if (cfAdmin != null) {
            cfAdmin.destroy();
        }
    }

}
