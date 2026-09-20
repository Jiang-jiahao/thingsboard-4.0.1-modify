package com.jnks.iot.server.edqs.processor;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.RecordTooLargeException;
import com.jnks.iot.server.common.data.ObjectType;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.edqs.state.EdqsPartitionService;
import com.jnks.iot.server.gen.transport.TransportProtos.ToEdqsMsg;
import com.jnks.iot.server.queue.TbQueueCallback;
import com.jnks.iot.server.queue.TbQueueMsgMetadata;
import com.jnks.iot.server.queue.TbQueueProducer;
import com.jnks.iot.server.queue.common.TbProtoQueueMsg;
import com.jnks.iot.server.queue.kafka.TbKafkaProducerTemplate;

@Slf4j
@Builder
@RequiredArgsConstructor
public class EdqsProducer {

    private final TbQueueProducer<TbProtoQueueMsg<ToEdqsMsg>> producer;
    private final EdqsPartitionService partitionService;

    public void send(TenantId tenantId, ObjectType type, String key, ToEdqsMsg msg) {
        TopicPartitionInfo tpi = TopicPartitionInfo.builder()
                .topic(producer.getDefaultTopic())
                .partition(partitionService.resolvePartition(tenantId, key))
                .build();
        TbQueueCallback callback = new TbQueueCallback() {
            @Override
            public void onSuccess(TbQueueMsgMetadata metadata) {
                log.trace("[{}][{}][{}] Published msg to {}: {}", tenantId, type, key, tpi, msg);
            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof RecordTooLargeException) {
                    if (!log.isDebugEnabled()) {
                        log.warn("[{}][{}][{}] Failed to publish msg to {}", tenantId, type, key, tpi, t); // not logging the whole message
                        return;
                    }
                }
                log.warn("[{}][{}][{}] Failed to publish msg to {}: {}", tenantId, type, key, tpi, msg, t);
            }
        };
        if (producer instanceof TbKafkaProducerTemplate<TbProtoQueueMsg<ToEdqsMsg>> kafkaProducer) {
            kafkaProducer.send(tpi, key, new TbProtoQueueMsg<>(null, msg), callback); // specifying custom key for compaction
        } else {
            producer.send(tpi, new TbProtoQueueMsg<>(null, msg), callback);
        }
    }

    public void stop() {
        producer.stop();
    }

}
