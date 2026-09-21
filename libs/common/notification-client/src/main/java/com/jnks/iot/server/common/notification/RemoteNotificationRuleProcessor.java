package com.jnks.iot.server.common.notification;

import com.google.protobuf.ByteString;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.JavaSerDesUtil;
import com.jnks.iot.server.common.data.notification.rule.trigger.NotificationRuleTrigger;
import com.jnks.iot.server.common.msg.notification.NotificationRuleProcessor;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.queue.discovery.PartitionService;
import com.jnks.iot.server.queue.discovery.TopicService;
import com.jnks.iot.server.queue.provider.JnksIotQueueProducerProvider;

import java.util.UUID;

@Service
@ConditionalOnMissingBean(value = NotificationRuleProcessor.class, ignored = RemoteNotificationRuleProcessor.class)
@RequiredArgsConstructor
@Slf4j
public class RemoteNotificationRuleProcessor implements NotificationRuleProcessor {

    private final NotificationDeduplicationService deduplicationService;
    private final JnksIotQueueProducerProvider producerProvider;
    private final TopicService topicService;
    private final PartitionService partitionService;

    @Override
    public void process(NotificationRuleTrigger trigger) {
        try {
            if (trigger.deduplicate() && deduplicationService.alreadyProcessed(trigger)) {
                return;
            }

            log.debug("Submitting notification rule trigger: {}", trigger);
            TransportProtos.NotificationRuleProcessorMsg.Builder msg = TransportProtos.NotificationRuleProcessorMsg.newBuilder()
                    .setTrigger(ByteString.copyFrom(JavaSerDesUtil.encode(trigger)));

            partitionService.getAllServiceIds(ServiceType.JNKS_IOT_CORE).stream().findAny().ifPresent(serviceId -> {
                TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.JNKS_IOT_CORE, serviceId);
                producerProvider.getJnksIotCoreNotificationsMsgProducer().send(tpi, new JnksIotProtoQueueMsg<>(UUID.randomUUID(),
                        TransportProtos.ToCoreNotificationMsg.newBuilder()
                                .setNotificationRuleProcessorMsg(msg)
                                .build()), null);
            });
        } catch (Throwable e) {
            log.error("Failed to submit notification rule trigger: {}", trigger, e);
        }
    }

}
