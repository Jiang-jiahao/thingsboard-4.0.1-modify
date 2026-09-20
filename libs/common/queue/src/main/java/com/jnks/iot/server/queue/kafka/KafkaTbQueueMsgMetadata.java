package com.jnks.iot.server.queue.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.kafka.clients.producer.RecordMetadata;
import com.jnks.iot.server.queue.TbQueueMsgMetadata;

@Data
@AllArgsConstructor
public class KafkaTbQueueMsgMetadata implements TbQueueMsgMetadata {
    private RecordMetadata metadata;
}
