package com.jnks.iot.server.queue.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.kafka.clients.producer.RecordMetadata;
import com.jnks.iot.server.queue.JnksIotQueueMsgMetadata;

@Data
@AllArgsConstructor
public class KafkaJnksIotQueueMsgMetadata implements JnksIotQueueMsgMetadata {
    private RecordMetadata metadata;
}
