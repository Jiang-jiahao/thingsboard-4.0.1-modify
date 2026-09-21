package com.jnks.iot.server.queue.provider;

import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCoreNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToHousekeeperServiceMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToUsageStatsServiceMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToVersionControlServiceMsg;
import com.jnks.iot.server.queue.JnksIotQueueAdmin;
import com.jnks.iot.server.queue.JnksIotQueueConsumer;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.queue.discovery.JnksIotServiceInfoProvider;
import com.jnks.iot.server.queue.discovery.TopicService;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaAdmin;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaConsumerStatsService;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaConsumerTemplate;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaProducerTemplate;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaSettings;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaTopicConfigs;
import com.jnks.iot.server.queue.settings.JnksIotQueueVersionControlSettings;

@Component
@ConditionalOnExpression("'${queue.type:null}'=='kafka'")
public class KafkaJnksIotVersionControlQueueFactory implements JnksIotVersionControlQueueFactory {

    private final JnksIotKafkaSettings kafkaSettings;
    private final JnksIotServiceInfoProvider serviceInfoProvider;
    private final String coreTopic;
    private final String coreUsageStatsTopic;
    private final String coreHousekeeperTopic;
    private final JnksIotQueueVersionControlSettings vcSettings;
    private final JnksIotKafkaConsumerStatsService consumerStatsService;
    private final TopicService topicService;

    private final JnksIotQueueAdmin coreAdmin;
    private final JnksIotQueueAdmin vcAdmin;
    private final JnksIotQueueAdmin notificationAdmin;
    private final JnksIotQueueAdmin housekeeperAdmin;

    public KafkaJnksIotVersionControlQueueFactory(JnksIotKafkaSettings kafkaSettings,
                                             JnksIotServiceInfoProvider serviceInfoProvider,
                                             @Value("${queue.core.topic}") String coreTopic,
                                             @Value("${queue.core.usage-stats-topic:jnks_iot_usage_stats}") String coreUsageStatsTopic,
                                             @Value("${queue.core.housekeeper.topic:jnks_iot_housekeeper}") String coreHousekeeperTopic,
                                             JnksIotQueueVersionControlSettings vcSettings,
                                             JnksIotKafkaConsumerStatsService consumerStatsService,
                                             JnksIotKafkaTopicConfigs kafkaTopicConfigs,
                                             TopicService topicService) {
        this.kafkaSettings = kafkaSettings;
        this.serviceInfoProvider = serviceInfoProvider;
        this.coreTopic = coreTopic;
        this.coreUsageStatsTopic = coreUsageStatsTopic;
        this.coreHousekeeperTopic = coreHousekeeperTopic;
        this.vcSettings = vcSettings;
        this.consumerStatsService = consumerStatsService;
        this.topicService = topicService;

        this.coreAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getCoreConfigs());
        this.vcAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getVcConfigs());
        this.notificationAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getNotificationsConfigs());
        this.housekeeperAdmin = new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getHousekeeperConfigs());
    }


    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> createJnksIotCoreNotificationsMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("jnks-iot-vc-to-core-notifications-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(coreTopic));
        requestBuilder.admin(notificationAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToVersionControlServiceMsg>> createToVersionControlMsgConsumer() {
        JnksIotKafkaConsumerTemplate.JnksIotKafkaConsumerTemplateBuilder<JnksIotProtoQueueMsg<ToVersionControlServiceMsg>> consumerBuilder = JnksIotKafkaConsumerTemplate.builder();
        consumerBuilder.settings(kafkaSettings);
        consumerBuilder.topic(topicService.buildTopicName(vcSettings.getTopic()));
        consumerBuilder.clientId("jnks-iot-vc-consumer-" + serviceInfoProvider.getServiceId());
        consumerBuilder.groupId(topicService.buildTopicName("jnks-iot-vc-node"));
        consumerBuilder.decoder(msg -> new JnksIotProtoQueueMsg<>(msg.getKey(), ToVersionControlServiceMsg.parseFrom(msg.getData()), msg.getHeaders()));
        consumerBuilder.admin(vcAdmin);
        consumerBuilder.statsService(consumerStatsService);
        return consumerBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToUsageStatsServiceMsg>> createToUsageStatsServiceMsgProducer() {
        JnksIotKafkaProducerTemplate.JnksIotKafkaProducerTemplateBuilder<JnksIotProtoQueueMsg<ToUsageStatsServiceMsg>> requestBuilder = JnksIotKafkaProducerTemplate.builder();
        requestBuilder.settings(kafkaSettings);
        requestBuilder.clientId("jnks-iot-vc-us-producer-" + serviceInfoProvider.getServiceId());
        requestBuilder.defaultTopic(topicService.buildTopicName(coreUsageStatsTopic));
        requestBuilder.admin(coreAdmin);
        return requestBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> createHousekeeperMsgProducer() {
        return JnksIotKafkaProducerTemplate.<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>>builder()
                .settings(kafkaSettings)
                .clientId("jnks-iot-vc-housekeeper-producer-" + serviceInfoProvider.getServiceId())
                .defaultTopic(topicService.buildTopicName(coreHousekeeperTopic))
                .admin(housekeeperAdmin)
                .build();
    }

    @PreDestroy
    private void destroy() {
        if (coreAdmin != null) {
            coreAdmin.destroy();
        }
        if (vcAdmin != null) {
            vcAdmin.destroy();
        }
        if (notificationAdmin != null) {
            notificationAdmin.destroy();
        }
    }
}
