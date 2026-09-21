package com.jnks.iot.server.queue.provider;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCoreMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCoreNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToRuleEngineMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToTransportMsg;
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
import com.jnks.iot.server.queue.memory.InMemoryStorage;
import com.jnks.iot.server.queue.memory.InMemoryJnksIotQueueConsumer;
import com.jnks.iot.server.queue.memory.InMemoryJnksIotQueueProducer;
import com.jnks.iot.server.queue.settings.JnksIotQueueTransportApiSettings;
import com.jnks.iot.server.queue.settings.JnksIotQueueTransportNotificationSettings;

@Component
@ConditionalOnExpression("'${queue.type:null}'=='in-memory' && '${transport.api_enabled:true}'=='true'")
@Slf4j
public class InMemoryJnksIotTransportQueueFactory implements JnksIotTransportQueueFactory {
    private final JnksIotQueueTransportApiSettings transportApiSettings;
    private final JnksIotQueueTransportNotificationSettings transportNotificationSettings;
    private final JnksIotServiceInfoProvider serviceInfoProvider;
    private final String coreTopic;
    private final String coreUsageStatsTopic;
    private final String coreHousekeeperTopic;
    private final InMemoryStorage storage;
    private final TopicService topicService;

    public InMemoryJnksIotTransportQueueFactory(JnksIotQueueTransportApiSettings transportApiSettings,
                                           JnksIotQueueTransportNotificationSettings transportNotificationSettings,
                                           JnksIotServiceInfoProvider serviceInfoProvider,
                                           @Value("${queue.core.topic}") String coreTopic,
                                           @Value("${queue.core.usage-stats-topic:jnks_iot_usage_stats}") String coreUsageStatsTopic,
                                           @Value("${queue.core.housekeeper.topic:jnks_iot_housekeeper}") String coreHousekeeperTopic,
                                           InMemoryStorage storage,
                                           TopicService topicService) {
        this.transportApiSettings = transportApiSettings;
        this.transportNotificationSettings = transportNotificationSettings;
        this.serviceInfoProvider = serviceInfoProvider;
        this.coreTopic = coreTopic;
        this.coreUsageStatsTopic = coreUsageStatsTopic;
        this.coreHousekeeperTopic = coreHousekeeperTopic;
        this.storage = storage;
        this.topicService = topicService;
    }

    @Override
    public JnksIotQueueRequestTemplate<JnksIotProtoQueueMsg<TransportApiRequestMsg>, JnksIotProtoQueueMsg<TransportApiResponseMsg>> createTransportApiRequestTemplate() {
        InMemoryJnksIotQueueProducer<JnksIotProtoQueueMsg<TransportApiRequestMsg>> producerTemplate =
                new InMemoryJnksIotQueueProducer<>(storage, topicService.buildTopicName(transportApiSettings.getRequestsTopic()));

        InMemoryJnksIotQueueConsumer<JnksIotProtoQueueMsg<TransportApiResponseMsg>> consumerTemplate =
                new InMemoryJnksIotQueueConsumer<>(storage, topicService.buildTopicName(transportApiSettings.getResponsesTopic() + "." + serviceInfoProvider.getServiceId()));

        DefaultJnksIotQueueRequestTemplate.DefaultJnksIotQueueRequestTemplateBuilder
                <JnksIotProtoQueueMsg<TransportApiRequestMsg>, JnksIotProtoQueueMsg<TransportApiResponseMsg>> templateBuilder = DefaultJnksIotQueueRequestTemplate.builder();

        templateBuilder.queueAdmin(new JnksIotQueueAdmin() {
            @Override
            public void createTopicIfNotExists(String topic, String properties) {}

            @Override
            public void destroy() {}

            @Override
            public void deleteTopic(String topic) {}
        });

        templateBuilder.requestTemplate(producerTemplate);
        templateBuilder.responseTemplate(consumerTemplate);
        templateBuilder.maxPendingRequests(transportApiSettings.getMaxPendingRequests());
        templateBuilder.maxRequestTimeout(transportApiSettings.getMaxRequestsTimeout());
        templateBuilder.pollInterval(transportApiSettings.getResponsePollInterval());
        return templateBuilder.build();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToRuleEngineMsg>> createRuleEngineMsgProducer() {
        return new InMemoryJnksIotQueueProducer<>(storage, topicService.buildTopicName(transportApiSettings.getRequestsTopic()));
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreMsg>> createJnksIotCoreMsgProducer() {
        return new InMemoryJnksIotQueueProducer<>(storage, topicService.buildTopicName(coreTopic));
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> createJnksIotCoreNotificationsMsgProducer() {
        return new InMemoryJnksIotQueueProducer<>(storage, topicService.buildTopicName(coreTopic));
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToTransportMsg>> createTransportNotificationsConsumer() {
        return new InMemoryJnksIotQueueConsumer<>(storage, topicService.buildTopicName(transportNotificationSettings.getNotificationsTopic() + "." + serviceInfoProvider.getServiceId()));
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<TransportProtos.ToUsageStatsServiceMsg>> createToUsageStatsServiceMsgProducer() {
        return new InMemoryJnksIotQueueProducer<>(storage, topicService.buildTopicName(coreUsageStatsTopic));
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<TransportProtos.ToHousekeeperServiceMsg>> createHousekeeperMsgProducer() {
        return new InMemoryJnksIotQueueProducer<>(storage, topicService.buildTopicName(coreHousekeeperTopic));
    }

}
