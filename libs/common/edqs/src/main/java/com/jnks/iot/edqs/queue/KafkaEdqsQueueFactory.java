package com.jnks.iot.edqs.queue;

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import com.jnks.iot.common.util.JnksIotExecutors;
import com.jnks.iot.server.common.stats.StatsFactory;
import com.jnks.iot.server.common.stats.StatsType;
import com.jnks.iot.server.gen.transport.TransportProtos.FromEdqsMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToEdqsMsg;
import com.jnks.iot.server.queue.TbQueueProducer;
import com.jnks.iot.server.queue.TbQueueResponseTemplate;
import com.jnks.iot.server.queue.common.DefaultTbQueueResponseTemplate;
import com.jnks.iot.server.queue.common.TbProtoQueueMsg;
import com.jnks.iot.server.queue.discovery.TbServiceInfoProvider;
import com.jnks.iot.server.queue.discovery.TopicService;
import com.jnks.iot.server.queue.kafka.TbKafkaAdmin;
import com.jnks.iot.server.queue.kafka.TbKafkaConsumerStatsService;
import com.jnks.iot.server.queue.kafka.TbKafkaConsumerTemplate;
import com.jnks.iot.server.queue.kafka.TbKafkaProducerTemplate;
import com.jnks.iot.server.queue.kafka.TbKafkaSettings;
import com.jnks.iot.server.queue.edqs.EdqsConfig;
import com.jnks.iot.server.queue.edqs.EdqsQueueFactory;
import com.jnks.iot.server.queue.kafka.TbKafkaTopicConfigs;

import java.util.concurrent.atomic.AtomicInteger;

@Component
@ConditionalOnExpression("'${queue.edqs.sync.enabled:true}'=='true' && '${queue.edqs.mode:null}'=='local' && '${queue.type:null}'=='kafka'")
public class KafkaEdqsQueueFactory implements EdqsQueueFactory {

    private final TbKafkaSettings kafkaSettings;
    private final TbKafkaAdmin edqsEventsAdmin;
    private final TbKafkaAdmin edqsRequestsAdmin;
    private final TbKafkaAdmin edqsStateAdmin;
    private final EdqsConfig edqsConfig;
    private final TbServiceInfoProvider serviceInfoProvider;
    private final TbKafkaConsumerStatsService consumerStatsService;
    private final TopicService topicService;
    private final StatsFactory statsFactory;

    private final AtomicInteger consumerCounter = new AtomicInteger();

    public KafkaEdqsQueueFactory(TbKafkaSettings kafkaSettings, TbKafkaTopicConfigs topicConfigs,
                                 EdqsConfig edqsConfig, TbServiceInfoProvider serviceInfoProvider,
                                 TbKafkaConsumerStatsService consumerStatsService, TopicService topicService,
                                 StatsFactory statsFactory) {
        this.edqsEventsAdmin = new TbKafkaAdmin(kafkaSettings, topicConfigs.getEdqsEventsConfigs());
        this.edqsRequestsAdmin = new TbKafkaAdmin(kafkaSettings, topicConfigs.getEdqsRequestsConfigs());
        this.edqsStateAdmin = new TbKafkaAdmin(kafkaSettings, topicConfigs.getEdqsStateConfigs());
        this.kafkaSettings = kafkaSettings;
        this.edqsConfig = edqsConfig;
        this.serviceInfoProvider = serviceInfoProvider;
        this.consumerStatsService = consumerStatsService;
        this.topicService = topicService;
        this.statsFactory = statsFactory;
    }

    @Override
    public TbKafkaConsumerTemplate<TbProtoQueueMsg<ToEdqsMsg>> createEdqsEventsConsumer() {
        return createEdqsMsgConsumer(edqsConfig.getEventsTopic(),
                "edqs-events-" + consumerCounter.getAndIncrement() + "-consumer-" + serviceInfoProvider.getServiceId(),
                null, // not using consumer group management, offsets from the edqs-events-to-backup-consumer-group are used (see KafkaEdqsStateService)
                false, edqsEventsAdmin);
    }

    @Override
    public TbKafkaConsumerTemplate<TbProtoQueueMsg<ToEdqsMsg>> createEdqsEventsToBackupConsumer() {
        return createEdqsMsgConsumer(edqsConfig.getEventsTopic(),
                "edqs-events-to-backup-consumer-" + serviceInfoProvider.getServiceId(),
                "edqs-events-to-backup-consumer-group",
                false, edqsEventsAdmin);
    }

    @Override
    public TbKafkaConsumerTemplate<TbProtoQueueMsg<ToEdqsMsg>> createEdqsStateConsumer() {
        return createEdqsMsgConsumer(edqsConfig.getStateTopic(),
                "edqs-state-" + consumerCounter.getAndIncrement() + "-consumer-" + serviceInfoProvider.getServiceId(),
                null, // not using consumer group management
                true, edqsStateAdmin);
    }

    public TbKafkaConsumerTemplate<TbProtoQueueMsg<ToEdqsMsg>> createEdqsMsgConsumer(String topic, String clientId, String group, boolean readFullAndStop, TbKafkaAdmin admin) {
        return TbKafkaConsumerTemplate.<TbProtoQueueMsg<ToEdqsMsg>>builder()
                .settings(kafkaSettings)
                .topic(topicService.buildTopicName(topic))
                .readFromBeginning(readFullAndStop)
                .stopWhenRead(readFullAndStop)
                .clientId(clientId)
                .groupId(topicService.buildTopicName(group))
                .decoder(msg -> new TbProtoQueueMsg<>(msg.getKey(), ToEdqsMsg.parseFrom(msg.getData()), msg.getHeaders()))
                .admin(admin)
                .statsService(consumerStatsService)
                .build();
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToEdqsMsg>> createEdqsStateProducer() {
        return TbKafkaProducerTemplate.<TbProtoQueueMsg<ToEdqsMsg>>builder()
                .clientId("edqs-state-producer-" + serviceInfoProvider.getServiceId())
                .defaultTopic(topicService.buildTopicName(edqsConfig.getStateTopic()))
                .settings(kafkaSettings)
                .admin(edqsStateAdmin)
                .build();
    }

    @Override
    public TbQueueResponseTemplate<TbProtoQueueMsg<ToEdqsMsg>, TbProtoQueueMsg<FromEdqsMsg>> createEdqsResponseTemplate() {
        var requestConsumer = createEdqsMsgConsumer(edqsConfig.getRequestsTopic(),
                "edqs-requests-consumer-" + serviceInfoProvider.getServiceId(),
                "edqs-requests-consumer-group",
                false, edqsRequestsAdmin);
        var responseProducer = TbKafkaProducerTemplate.<TbProtoQueueMsg<FromEdqsMsg>>builder()
                .settings(kafkaSettings)
                .clientId("edqs-response-producer-" + serviceInfoProvider.getServiceId())
                .defaultTopic(topicService.buildTopicName(edqsConfig.getResponsesTopic()))
                .admin(edqsRequestsAdmin)
                .build();
        return DefaultTbQueueResponseTemplate.<TbProtoQueueMsg<ToEdqsMsg>, TbProtoQueueMsg<FromEdqsMsg>>builder()
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
    public TbKafkaAdmin getEdqsQueueAdmin() {
        return edqsEventsAdmin;
    }

}
