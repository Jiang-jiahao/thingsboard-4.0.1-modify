package com.jnks.iot.server.queue.kafka;

/**
 * Created by ashvayka on 25.09.18.
 */
public interface JnksIotKafkaEncoder<T> {

    byte[] encode(T value);

}
