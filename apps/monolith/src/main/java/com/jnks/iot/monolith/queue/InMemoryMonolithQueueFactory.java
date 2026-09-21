package com.jnks.iot.monolith.queue;

import com.jnks.iot.server.queue.provider.*;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.queue.Queue;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.gen.js.JsInvokeProtos;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.gen.transport.TransportProtos.CalculatedFieldStateProto;
import com.jnks.iot.server.gen.transport.TransportProtos.FromEdqsMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToEdqsMsg;
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
import com.jnks.iot.server.queue.memory.InMemoryStorage;
import com.jnks.iot.server.queue.memory.InMemoryJnksIotQueueConsumer;
import com.jnks.iot.server.queue.memory.InMemoryJnksIotQueueProducer;
import com.jnks.iot.server.queue.settings.JnksIotQueueCalculatedFieldSettings;
import com.jnks.iot.server.queue.settings.JnksIotQueueCoreSettings;
import com.jnks.iot.server.queue.settings.JnksIotQueueRuleEngineSettings;
import com.jnks.iot.server.queue.settings.JnksIotQueueTransportApiSettings;
import com.jnks.iot.server.queue.settings.JnksIotQueueTransportNotificationSettings;
import com.jnks.iot.server.queue.settings.JnksIotQueueVersionControlSettings;

/**
 * 内存队列实现，用于单体部署
 */
@Slf4j
@Component
@ConditionalOnExpression("'${queue.type:null}'=='in-memory'")
@RequiredArgsConstructor
public class InMemoryMonolithQueueFactory implements JnksIotCoreQueueFactory, JnksIotRuleEngineQueueFactory, JnksIotVersionControlQueueFactory {

    private final TopicService topicService;
    private final JnksIotQueueCoreSettings coreSettings;
    private final JnksIotServiceInfoProvider serviceInfoProvider;
    private final JnksIotQueueAdmin queueAdmin;
    private final JnksIotQueueRuleEngineSettings ruleEngineSettings;
    private final JnksIotQueueVersionControlSettings vcSettings;
    private final JnksIotQueueTransportApiSettings transportApiSettings;
    private final JnksIotQueueTransportNotificationSettings transportNotificationSettings;
    private final JnksIotQueueCalculatedFieldSettings calculatedFieldSettings;
    private final EdqsConfig edqsConfig;
    private final InMemoryStorage storage;

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<TransportProtos.ToTransportMsg>> createTransportNotificationsMsgProducer() {
        return new InMemoryJnksIotQueueProducer<>(storage, topicService.buildTopicName(transportNotificationSettings.getNotificationsTopic()));
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> createRuleEngineMsgProducer() {
        return new InMemoryJnksIotQueueProducer<>(storage, topicService.buildTopicName(ruleEngineSettings.getTopic()));
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<TransportProtos.ToRuleEngineNotificationMsg>> createRuleEngineNotificationsMsgProducer() {
        return new InMemoryJnksIotQueueProducer<>(storage, topicService.buildTopicName(ruleEngineSettings.getTopic()));
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<TransportProtos.ToCoreMsg>> createJnksIotCoreMsgProducer() {
        return new InMemoryJnksIotQueueProducer<>(storage, topicService.buildTopicName(coreSettings.getTopic()));
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<TransportProtos.ToCoreNotificationMsg>> createJnksIotCoreNotificationsMsgProducer() {
        return new InMemoryJnksIotQueueProducer<>(storage, topicService.getNotificationsTopic(ServiceType.JNKS_IOT_CORE, serviceInfoProvider.getServiceId()).getFullTopicName());
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<TransportProtos.ToVersionControlServiceMsg>> createToVersionControlMsgConsumer() {
        return new InMemoryJnksIotQueueConsumer<>(storage, topicService.buildTopicName(vcSettings.getTopic()));
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> createToRuleEngineMsgConsumer(Queue configuration) {
        return new InMemoryJnksIotQueueConsumer<>(storage, topicService.buildTopicName(configuration.getTopic()));
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<TransportProtos.ToRuleEngineNotificationMsg>> createToRuleEngineNotificationsMsgConsumer() {
        return new InMemoryJnksIotQueueConsumer<>(storage, topicService.getNotificationsTopic(ServiceType.JNKS_IOT_RULE_ENGINE, serviceInfoProvider.getServiceId()).getFullTopicName());
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<TransportProtos.ToCoreMsg>> createToCoreMsgConsumer() {
        return new InMemoryJnksIotQueueConsumer<>(storage, topicService.buildTopicName(coreSettings.getTopic()));
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<TransportProtos.ToCoreNotificationMsg>> createToCoreNotificationsMsgConsumer() {
        return new InMemoryJnksIotQueueConsumer<>(storage, topicService.getNotificationsTopic(ServiceType.JNKS_IOT_CORE, serviceInfoProvider.getServiceId()).getFullTopicName());
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<TransportProtos.TransportApiRequestMsg>> createTransportApiRequestConsumer() {
        return new InMemoryJnksIotQueueConsumer<>(storage, topicService.buildTopicName(transportApiSettings.getRequestsTopic()));
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<TransportProtos.TransportApiResponseMsg>> createTransportApiResponseProducer() {
        return new InMemoryJnksIotQueueProducer<>(storage, topicService.buildTopicName(transportApiSettings.getResponsesTopic()));
    }

    @Override
    public JnksIotQueueRequestTemplate<JnksIotProtoJsQueueMsg<JsInvokeProtos.RemoteJsRequest>, JnksIotProtoQueueMsg<JsInvokeProtos.RemoteJsResponse>> createRemoteJsRequestTemplate() {
        return null;
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<TransportProtos.ToCalculatedFieldMsg>> createToCalculatedFieldMsgConsumer(TopicPartitionInfo tpi) {
        return new InMemoryJnksIotQueueConsumer<>(storage, topicService.buildTopicName(calculatedFieldSettings.getEventTopic()));
    }

    @Override
    public JnksIotQueueAdmin getCalculatedFieldQueueAdmin() {
        return queueAdmin;
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<TransportProtos.ToCalculatedFieldMsg>> createToCalculatedFieldMsgProducer() {
        return new InMemoryJnksIotQueueProducer<>(storage, topicService.buildTopicName(calculatedFieldSettings.getEventTopic()));
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<TransportProtos.ToCalculatedFieldNotificationMsg>> createToCalculatedFieldNotificationMsgConsumer() {
        return new InMemoryJnksIotQueueConsumer<>(storage, topicService.getCalculatedFieldNotificationsTopic(serviceInfoProvider.getServiceId()).getFullTopicName());
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<CalculatedFieldStateProto>> createCalculatedFieldStateConsumer() {
        return new InMemoryJnksIotQueueConsumer<>(storage, topicService.buildTopicName(calculatedFieldSettings.getStateTopic()));
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<CalculatedFieldStateProto>> createCalculatedFieldStateProducer() {
        return new InMemoryJnksIotQueueProducer<>(storage, topicService.buildTopicName(calculatedFieldSettings.getStateTopic()));
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<TransportProtos.ToUsageStatsServiceMsg>> createToUsageStatsServiceMsgConsumer() {
        return new InMemoryJnksIotQueueConsumer<>(storage, topicService.buildTopicName(coreSettings.getUsageStatsTopic()));
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<TransportProtos.ToOtaPackageStateServiceMsg>> createToOtaPackageStateServiceMsgConsumer() {
        return new InMemoryJnksIotQueueConsumer<>(storage, topicService.buildTopicName(coreSettings.getOtaPackageTopic()));
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<TransportProtos.ToOtaPackageStateServiceMsg>> createToOtaPackageStateServiceMsgProducer() {
        return new InMemoryJnksIotQueueProducer<>(storage, topicService.buildTopicName(coreSettings.getOtaPackageTopic()));
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<TransportProtos.ToUsageStatsServiceMsg>> createToUsageStatsServiceMsgProducer() {
        return new InMemoryJnksIotQueueProducer<>(storage, topicService.buildTopicName(coreSettings.getUsageStatsTopic()));
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<TransportProtos.ToVersionControlServiceMsg>> createVersionControlMsgProducer() {
        return new InMemoryJnksIotQueueProducer<>(storage, topicService.buildTopicName(vcSettings.getTopic()));
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<TransportProtos.ToHousekeeperServiceMsg>> createHousekeeperMsgProducer() {
        return new InMemoryJnksIotQueueProducer<>(storage, topicService.buildTopicName(coreSettings.getHousekeeperTopic()));
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<TransportProtos.ToHousekeeperServiceMsg>> createHousekeeperMsgConsumer() {
        return new InMemoryJnksIotQueueConsumer<>(storage, topicService.buildTopicName(coreSettings.getHousekeeperTopic()));
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<TransportProtos.ToHousekeeperServiceMsg>> createHousekeeperReprocessingMsgProducer() {
        return new InMemoryJnksIotQueueProducer<>(storage, topicService.buildTopicName(coreSettings.getHousekeeperReprocessingTopic()));
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<TransportProtos.ToHousekeeperServiceMsg>> createHousekeeperReprocessingMsgConsumer() {
        return new InMemoryJnksIotQueueConsumer<>(storage, topicService.buildTopicName(coreSettings.getHousekeeperReprocessingTopic()));
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<TransportProtos.ToCalculatedFieldNotificationMsg>> createToCalculatedFieldNotificationMsgProducer() {
        return new InMemoryJnksIotQueueProducer<>(storage, topicService.buildTopicName(calculatedFieldSettings.getEventTopic()));
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToEdqsMsg>> createEdqsEventsProducer() {
        return new InMemoryJnksIotQueueProducer<>(storage, edqsConfig.getEventsTopic());
    }

    @Override
    public JnksIotQueueRequestTemplate<JnksIotProtoQueueMsg<ToEdqsMsg>, JnksIotProtoQueueMsg<FromEdqsMsg>> createEdqsRequestTemplate() {
        JnksIotQueueProducer<JnksIotProtoQueueMsg<ToEdqsMsg>> requestProducer = new InMemoryJnksIotQueueProducer<>(storage, edqsConfig.getRequestsTopic());
        JnksIotQueueConsumer<JnksIotProtoQueueMsg<FromEdqsMsg>> responseConsumer = new InMemoryJnksIotQueueConsumer<>(storage, edqsConfig.getResponsesTopic());

        return DefaultJnksIotQueueRequestTemplate.<JnksIotProtoQueueMsg<ToEdqsMsg>, JnksIotProtoQueueMsg<FromEdqsMsg>>builder()
                .queueAdmin(queueAdmin)
                .requestTemplate(requestProducer)
                .responseTemplate(responseConsumer)
                .maxPendingRequests(edqsConfig.getMaxPendingRequests())
                .maxRequestTimeout(edqsConfig.getMaxRequestTimeout())
                .pollInterval(edqsConfig.getPollInterval())
                .build();
    }

    @Scheduled(fixedRateString = "${queue.in_memory.stats.print-interval-ms:60000}")
    private void printInMemoryStats() {
        storage.printStats();
    }

}
