package com.jnks.iot.server.service.cf.ctx.state;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.DataConstants;
import com.jnks.iot.server.common.data.id.CalculatedFieldId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.EntityIdFactory;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.common.msg.queue.JnksIotCallback;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.gen.transport.TransportProtos.CalculatedFieldStateProto;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCalculatedFieldMsg;
import com.jnks.iot.server.queue.JnksIotQueueCallback;
import com.jnks.iot.server.queue.JnksIotQueueMsgHeaders;
import com.jnks.iot.server.queue.JnksIotQueueMsgMetadata;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.queue.common.consumer.PartitionedQueueConsumerManager;
import com.jnks.iot.server.queue.common.state.KafkaQueueStateService;
import com.jnks.iot.server.queue.discovery.PartitionService;
import com.jnks.iot.server.queue.discovery.QueueKey;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaProducerTemplate;
import com.jnks.iot.server.queue.provider.JnksIotRuleEngineQueueFactory;
import com.jnks.iot.server.service.cf.ctx.AbstractCalculatedFieldStateService;
import com.jnks.iot.server.service.cf.ctx.CalculatedFieldEntityCtxId;

import java.util.concurrent.atomic.AtomicInteger;

import static com.jnks.iot.server.queue.common.AbstractJnksIotQueueTemplate.bytesToString;
import static com.jnks.iot.server.queue.common.AbstractJnksIotQueueTemplate.bytesToUuid;
import static com.jnks.iot.server.queue.common.AbstractJnksIotQueueTemplate.stringToBytes;
import static com.jnks.iot.server.queue.common.AbstractJnksIotQueueTemplate.uuidToBytes;

@Service
@RequiredArgsConstructor
@Slf4j
@ConditionalOnExpression("'${queue.type:null}'=='kafka'")
public class KafkaCalculatedFieldStateService extends AbstractCalculatedFieldStateService {

    private final JnksIotRuleEngineQueueFactory queueFactory;
    private final PartitionService partitionService;

    @Value("${queue.calculated_fields.poll_interval:25}")
    private long pollInterval;

    private JnksIotKafkaProducerTemplate<JnksIotProtoQueueMsg<CalculatedFieldStateProto>> stateProducer;

    private final AtomicInteger counter = new AtomicInteger();

    @Override
    public void init(PartitionedQueueConsumerManager<JnksIotProtoQueueMsg<ToCalculatedFieldMsg>> eventConsumer) {
        var queueKey = new QueueKey(ServiceType.JNKS_IOT_RULE_ENGINE, DataConstants.CF_STATES_QUEUE_NAME);
        PartitionedQueueConsumerManager<JnksIotProtoQueueMsg<CalculatedFieldStateProto>> stateConsumer = PartitionedQueueConsumerManager.<JnksIotProtoQueueMsg<CalculatedFieldStateProto>>create()
                .queueKey(queueKey)
                .topic(partitionService.getTopic(queueKey))
                .pollInterval(pollInterval)
                .msgPackProcessor((msgs, consumer, config) -> {
                    for (JnksIotProtoQueueMsg<CalculatedFieldStateProto> msg : msgs) {
                        try {
                            if (msg.getValue() != null) {
                                processRestoredState(msg.getValue());
                            } else {
                                processRestoredState(getStateId(msg.getHeaders()), null);
                            }
                        } catch (Throwable t) {
                            log.error("Failed to process state message: {}", msg, t);
                        }

                        int processedMsgCount = counter.incrementAndGet();
                        if (processedMsgCount % 10000 == 0) {
                            log.info("Processed {} calculated field state msgs", processedMsgCount);
                        }
                    }
                })
                .consumerCreator((queueConfig, tpi) -> queueFactory.createCalculatedFieldStateConsumer())
                .queueAdmin(queueFactory.getCalculatedFieldQueueAdmin())
                .consumerExecutor(eventConsumer.getConsumerExecutor())
                .scheduler(eventConsumer.getScheduler())
                .taskExecutor(eventConsumer.getTaskExecutor())
                .build();
        super.stateService = KafkaQueueStateService.<JnksIotProtoQueueMsg<ToCalculatedFieldMsg>, JnksIotProtoQueueMsg<CalculatedFieldStateProto>>builder()
                .eventConsumer(eventConsumer)
                .stateConsumer(stateConsumer)
                .build();
        this.stateProducer = (JnksIotKafkaProducerTemplate<JnksIotProtoQueueMsg<CalculatedFieldStateProto>>) queueFactory.createCalculatedFieldStateProducer();
    }

    @Override
    protected void doPersist(CalculatedFieldEntityCtxId stateId, CalculatedFieldStateProto stateMsgProto, JnksIotCallback callback) {
        TopicPartitionInfo tpi = partitionService.resolve(ServiceType.JNKS_IOT_RULE_ENGINE, DataConstants.CF_STATES_QUEUE_NAME, stateId.tenantId(), stateId.entityId());
        JnksIotProtoQueueMsg<CalculatedFieldStateProto> msg = new JnksIotProtoQueueMsg<>(stateId.entityId().getId(), stateMsgProto);
        if (stateMsgProto == null) {
            putStateId(msg.getHeaders(), stateId);
        }
        stateProducer.send(tpi, stateId.toKey(), msg, new JnksIotQueueCallback() {
            @Override
            public void onSuccess(JnksIotQueueMsgMetadata metadata) {
                if (callback != null) {
                    callback.onSuccess();
                }
            }

            @Override
            public void onFailure(Throwable t) {
                if (callback != null) {
                    callback.onFailure(t);
                }
            }
        });
    }

    @Override
    protected void doRemove(CalculatedFieldEntityCtxId stateId, JnksIotCallback callback) {
        doPersist(stateId, null, callback);
    }

    private void putStateId(JnksIotQueueMsgHeaders headers, CalculatedFieldEntityCtxId stateId) {
        headers.put("tenantId", uuidToBytes(stateId.tenantId().getId()));
        headers.put("cfId", uuidToBytes(stateId.cfId().getId()));
        headers.put("entityId", uuidToBytes(stateId.entityId().getId()));
        headers.put("entityType", stringToBytes(stateId.entityId().getEntityType().name()));
    }

    private CalculatedFieldEntityCtxId getStateId(JnksIotQueueMsgHeaders headers) {
        TenantId tenantId = TenantId.fromUUID(bytesToUuid(headers.get("tenantId")));
        CalculatedFieldId cfId = new CalculatedFieldId(bytesToUuid(headers.get("cfId")));
        EntityId entityId = EntityIdFactory.getByTypeAndUuid(bytesToString(headers.get("entityType")), bytesToUuid(headers.get("entityId")));
        return new CalculatedFieldEntityCtxId(tenantId, cfId, entityId);
    }

    @Override
    public void stop() {
        super.stop();
        stateProducer.stop();
    }

}
