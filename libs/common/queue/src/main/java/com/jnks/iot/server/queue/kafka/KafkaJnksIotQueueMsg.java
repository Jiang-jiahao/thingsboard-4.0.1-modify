package com.jnks.iot.server.queue.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.jnks.iot.server.queue.JnksIotQueueMsg;
import com.jnks.iot.server.queue.JnksIotQueueMsgHeaders;
import com.jnks.iot.server.queue.common.DefaultJnksIotQueueMsgHeaders;

import java.util.UUID;

public class KafkaJnksIotQueueMsg implements JnksIotQueueMsg {

    private static final int UUID_LENGTH = 36;

    private final UUID key;
    private final JnksIotQueueMsgHeaders headers;
    private final byte[] data;

    public KafkaJnksIotQueueMsg(ConsumerRecord<String, byte[]> record) {
        if (record.key().length() <= UUID_LENGTH) {
            this.key = UUID.fromString(record.key());
        } else {
            this.key = UUID.randomUUID();
        }
        JnksIotQueueMsgHeaders headers = new DefaultJnksIotQueueMsgHeaders();
        record.headers().forEach(header -> {
            headers.put(header.key(), header.value());
        });
        this.headers = headers;
        this.data = record.value();
    }

    @Override
    public UUID getKey() {
        return key;
    }

    @Override
    public JnksIotQueueMsgHeaders getHeaders() {
        return headers;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}
