package com.jnks.iot.edqs.queue;

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import com.jnks.iot.common.util.JnksIotExecutors;
import com.jnks.iot.server.common.stats.StatsFactory;
import com.jnks.iot.server.common.stats.StatsType;
import com.jnks.iot.server.gen.transport.TransportProtos.FromEdqsMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToEdqsMsg;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.JnksIotQueueResponseTemplate;
import com.jnks.iot.server.queue.common.DefaultJnksIotQueueResponseTemplate;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.queue.discovery.JnksIotServiceInfoProvider;
import com.jnks.iot.server.queue.discovery.TopicService;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaAdmin;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaConsumerStatsService;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaConsumerTemplate;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaProducerTemplate;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaSettings;
import com.jnks.iot.server.queue.edqs.EdqsConfig;
import com.jnks.iot.server.queue.edqs.EdqsQueueFactory;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaTopicConfigs;

import java.util.concurrent.atomic.AtomicInteger;

@Component
@ConditionalOnExpression("'${queue.edqs.sync.enabled:true}'=='true' && '${queue.edqs.mode:null}'=='local' && '${queue.type:null}'=='kafka'")
public class KafkaEdqsQueueFactory implements EdqsQueueFactory {

    private final JnksIotKafkaSettings kafkaSettings;
    private final JnksIotKafkaAdmin edqsEventsAdmin;
    private final JnksIotKafkaAdmin edqsRequestsAdmin;
    private final JnksIotKafkaAdmin edqsStateAdmin;
    private final EdqsConfig edqsConfig;
    private final JnksIotServiceInfoProvider serviceInfoProvider;
    private final JnksIotKafkaConsumerStatsService consumerStatsService;
    private final TopicService topicService;
    private final StatsFactory statsFactory;

    private final AtomicInteger consumerCounter = new AtomicInteger();

    public KafkaEdqsQueueFactory(JnksIotKafkaSettings kafkaSettings, JnksIotKafkaTopicConfigs topicConfigs,
                                 EdqsConfig edqsConfig, JnksIotServiceInfoProvider serviceInfoProvider,
                                 JnksIotKafkaConsumerStatsService consumerStatsService, TopicService topicService,
                                 StatsFactory statsFactory) {
        this.edqsEventsAdmin = new JnksIotKafkaAdmin(kafkaSettings, topicConfigs.getEdqsEventsConfigs());
        this.edqsRequestsAdmin = new JnksIotKafkaAdmin(kafkaSettings, topicConfigs.getEdqsRequestsConfigs());
        this.edqsStateAdmin = new JnksIotKafkaAdmin(kafkaSettings, topicConfigs.getEdqsStateConfigs());
        this.kafkaSettings = kafkaSettings;
        this.edqsConfig = edqsConfig;
        this.serviceInfoProvider = serviceInfoProvider;
        this.consumerStatsService = consumerStatsService;
        this.topicService = topicService;
        this.statsFactory = statsFactory;
    }

    @Override
    public JnksIotKafkaConsumerTemplate<JnksIotProtoQueueMsg<ToEdqsMsg>> createEdqsEventsConsumer() {
        return createEdqsMsgConsumer(edqsConfig.getEventsTopic(),
                "edqs-events-" + consumerCounter.getAndIncrement() + "-consumer-" + serviceInfoProvider.getServiceId(),
                null, // not using consumer group management, offsets from the edqs-events-to-backup-consumer-group are used (see KafkaEdqsStateService)
                false, edqsEventsAdmin);
    }

    @Override
    public JnksIotKafkaConsumerTemplate<JnksIotProtoQueueMsg<ToEdqsMsg>> createEdqsEventsToBackupConsumer() {
        return createEdqsMsgConsumer(edqsConfig.getEventsTopic(),
                "edqs-events-to-backup-consumer-" + serviceInfoProvider.getServiceId(),
                "edqs-events-to-backup-consumer-group",
                false, edqsEventsAdmin);
    }

    @Override
    public JnksIotKafkaConsumerTemplate<JnksIotProtoQueueMsg<ToEdqsMsg>> createEdqsStateConsumer() {
        return createEdqsMsgConsumer(edqsConfig.getStateTopic(),
                "edqs-state-" + consumerCounter.getAndIncrement() + "-consumer-" + serviceInfoProvider.getServiceId(),
                null, // not using consumer group management
                true, edqsStateAdmin);
    }

    public JnksIotKafkaConsumerTemplate<JnksIotProtoQueueMsg<ToEdqsMsg>> createEdqsMsgConsumer(String topic, String clientId, String group, boolean readFullAndStop, JnksIotKafkaAdmin admin) {
        return JnksIotKafkaConsumerTemplate.<JnksIotProtoQueueMsg<ToEdqsMsg>>builder()
                .settings(kafkaSettings)
                .topic(topicService.buildTopicName(topic))
                .readFromBeginning(readFullAndStop)
                .stopWhenRead(readFullAndStop)
                .clientId(clientId)
                .groupId(topicService.buildTopicName(group))
                .decoder(msg -> new JnksIotProtoQueueMsg<>(msg.getKey(), ToEdqsMsg.parseFrom(msg.getData()), msg.getHeaders()))
                .admin(admin)
                .statsService(consumerStatsService)
                .build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToEdqsMsg>> createEdqsStateProducer() {
        return JnksIotKafkaProducerTemplate.<JnksIotProtoQueueMsg<ToEdqsMsg>>builder()
                .clientId("edqs-state-producer-" + serviceInfoProvider.getServiceId())
                .defaultTopic(topicService.buildTopicName(edqsConfig.getStateTopic()))
                .settings(kafkaSettings)
                .admin(edqsStateAdmin)
                .build();
    }

    @Override
    public JnksIotQueueResponseTemplate<JnksIotProtoQueueMsg<ToEdqsMsg>, JnksIotProtoQueueMsg<FromEdqsMsg>> createEdqsResponseTemplate() {
        var requestConsumer = createEdqsMsgConsumer(edqsConfig.getRequestsTopic(),
                "edqs-requests-consumer-" + serviceInfoProvider.getServiceId(),
                "edqs-requests-consumer-group",
                false, edqsRequestsAdmin);
        var responseProducer = JnksIotKafkaProducerTemplate.<JnksIotProtoQueueMsg<FromEdqsMsg>>builder()
                .settings(kafkaSettings)
                .clientId("edqs-response-producer-" + serviceInfoProvider.getServiceId())
                .defaultTopic(topicService.buildTopicName(edqsConfig.getResponsesTopic()))
                .admin(edqsRequestsAdmin)
                .build();
        return DefaultJnksIotQueueResponseTemplate.<JnksIotProtoQueueMsg<ToEdqsMsg>, JnksIotProtoQueueMsg<FromEdqsMsg>>builder()
                .requestTemplate(requestConsumer)
                .responseTemplate(responseProducer)
                .maxPendingRequests(edqsConfig.getMaxPendingRequests())
                .requestTimeout(edqsConfig.getMaxRequestTimeout())
                .pollInterval(edqsConfig.getPollInterval())
                .stats(statsFactory.createMessagesStats(StatsType.EDQS.getName()))
                .executor(JnksIotExecutors.newWorkStealingPool(5, "edqs"))
                .build();
    }

    @Override
    public JnksIotKafkaAdmin getEdqsQueueAdmin() {
        return edqsEventsAdmin;
    }

}
