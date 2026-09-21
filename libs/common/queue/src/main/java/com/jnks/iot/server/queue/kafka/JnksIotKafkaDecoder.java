package com.jnks.iot.server.queue.kafka;

import com.jnks.iot.server.queue.JnksIotQueueMsg;

import java.io.IOException;

/**
 * Created by ashvayka on 25.09.18.
 */
public interface JnksIotKafkaDecoder<T> {

    T decode(JnksIotQueueMsg msg) throws IOException;

}
