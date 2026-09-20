package com.jnks.iot.server.queue.kafka;

import com.jnks.iot.server.queue.TbQueueMsg;

import java.io.IOException;

/**
 * Created by ashvayka on 25.09.18.
 */
public interface TbKafkaDecoder<T> {

    T decode(TbQueueMsg msg) throws IOException;

}
