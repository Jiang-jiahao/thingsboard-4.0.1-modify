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
import com.jnks.iot.server.common.msg.queue.TbCallback;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.gen.transport.TransportProtos.CalculatedFieldStateProto;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCalculatedFieldMsg;
import com.jnks.iot.server.queue.TbQueueCallback;
import com.jnks.iot.server.queue.TbQueueMsgHeaders;
import com.jnks.iot.server.queue.TbQueueMsgMetadata;
import com.jnks.iot.server.queue.common.TbProtoQueueMsg;
import com.jnks.iot.server.queue.common.consumer.PartitionedQueueConsumerManager;
import com.jnks.iot.server.queue.common.state.KafkaQueueStateService;
import com.jnks.iot.server.queue.discovery.PartitionService;
import com.jnks.iot.server.queue.discovery.QueueKey;
import com.jnks.iot.server.queue.kafka.TbKafkaProducerTemplate;
import com.jnks.iot.server.queue.provider.TbRuleEngineQueueFactory;
import com.jnks.iot.server.service.cf.ctx.AbstractCalculatedFieldStateService;
import com.jnks.iot.server.service.cf.ctx.CalculatedFieldEntityCtxId;

import java.util.concurrent.atomic.AtomicInteger;

import static com.jnks.iot.server.queue.common.AbstractTbQueueTemplate.bytesToString;
import static com.jnks.iot.server.queue.common.AbstractTbQueueTemplate.bytesToUuid;
import static com.jnks.iot.server.queue.common.AbstractTbQueueTemplate.stringToBytes;
import static com.jnks.iot.server.queue.common.AbstractTbQueueTemplate.uuidToBytes;

@Service
@RequiredArgsConstructor
@Slf4j
@ConditionalOnExpression("'${queue.type:null}'=='kafka'")
public class KafkaCalculatedFieldStateService extends AbstractCalculatedFieldStateService {

    private final TbRuleEngineQueueFactory queueFactory;
    private final PartitionService partitionService;

    @Value("${queue.calculated_fields.poll_interval:25}")
    private long pollInterval;

    private TbKafkaProducerTemplate<TbProtoQueueMsg<CalculatedFieldStateProto>> stateProducer;

    private final AtomicInteger counter = new AtomicInteger();

    @Override
    public void init(PartitionedQueueConsumerManager<TbProtoQueueMsg<ToCalculatedFieldMsg>> eventConsumer) {
        var queueKey = new QueueKey(ServiceType.TB_RULE_ENGINE, DataConstants.CF_STATES_QUEUE_NAME);
        PartitionedQueueConsumerManager<TbProtoQueueMsg<CalculatedFieldStateProto>> stateConsumer = PartitionedQueueConsumerManager.<TbProtoQueueMsg<CalculatedFieldStateProto>>create()
                .queueKey(queueKey)
                .topic(partitionService.getTopic(queueKey))
                .pollInterval(pollInterval)
                .msgPackProcessor((msgs, consumer, config) -> {
                    for (TbProtoQueueMsg<CalculatedFieldStateProto> msg : msgs) {
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
        super.stateService = KafkaQueueStateService.<TbProtoQueueMsg<ToCalculatedFieldMsg>, TbProtoQueueMsg<CalculatedFieldStateProto>>builder()
                .eventConsumer(eventConsumer)
                .stateConsumer(stateConsumer)
                .build();
        this.stateProducer = (TbKafkaProducerTemplate<TbProtoQueueMsg<CalculatedFieldStateProto>>) queueFactory.createCalculatedFieldStateProducer();
    }

    @Override
    protected void doPersist(CalculatedFieldEntityCtxId stateId, CalculatedFieldStateProto stateMsgProto, TbCallback callback) {
        TopicPartitionInfo tpi = partitionService.resolve(ServiceType.TB_RULE_ENGINE, DataConstants.CF_STATES_QUEUE_NAME, stateId.tenantId(), stateId.entityId());
        TbProtoQueueMsg<CalculatedFieldStateProto> msg = new TbProtoQueueMsg<>(stateId.entityId().getId(), stateMsgProto);
        if (stateMsgProto == null) {
            putStateId(msg.getHeaders(), stateId);
        }
        stateProducer.send(tpi, stateId.toKey(), msg, new TbQueueCallback() {
            @Override
            public void onSuccess(TbQueueMsgMetadata metadata) {
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
    protected void doRemove(CalculatedFieldEntityCtxId stateId, TbCallback callback) {
        doPersist(stateId, null, callback);
    }

    private void putStateId(TbQueueMsgHeaders headers, CalculatedFieldEntityCtxId stateId) {
        headers.put("tenantId", uuidToBytes(stateId.tenantId().getId()));
        headers.put("cfId", uuidToBytes(stateId.cfId().getId()));
        headers.put("entityId", uuidToBytes(stateId.entityId().getId()));
        headers.put("entityType", stringToBytes(stateId.entityId().getEntityType().name()));
    }

    private CalculatedFieldEntityCtxId getStateId(TbQueueMsgHeaders headers) {
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
