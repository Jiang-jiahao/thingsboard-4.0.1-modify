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
import com.jnks.iot.server.queue.JnksIotQueueCallback;
import com.jnks.iot.server.queue.JnksIotQueueMsgMetadata;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaProducerTemplate;

@Slf4j
@Builder
@RequiredArgsConstructor
public class EdqsProducer {

    private final JnksIotQueueProducer<JnksIotProtoQueueMsg<ToEdqsMsg>> producer;
    private final EdqsPartitionService partitionService;

    public void send(TenantId tenantId, ObjectType type, String key, ToEdqsMsg msg) {
        TopicPartitionInfo tpi = TopicPartitionInfo.builder()
                .topic(producer.getDefaultTopic())
                .partition(partitionService.resolvePartition(tenantId, key))
                .build();
        JnksIotQueueCallback callback = new JnksIotQueueCallback() {
            @Override
            public void onSuccess(JnksIotQueueMsgMetadata metadata) {
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
        if (producer instanceof JnksIotKafkaProducerTemplate<JnksIotProtoQueueMsg<ToEdqsMsg>> kafkaProducer) {
            kafkaProducer.send(tpi, key, new JnksIotProtoQueueMsg<>(null, msg), callback); // specifying custom key for compaction
        } else {
            producer.send(tpi, new JnksIotProtoQueueMsg<>(null, msg), callback);
        }
    }

    public void stop() {
        producer.stop();
    }

}
