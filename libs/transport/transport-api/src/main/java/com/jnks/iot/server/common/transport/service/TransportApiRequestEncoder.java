package com.jnks.iot.server.common.transport.service;

import com.jnks.iot.server.gen.transport.TransportProtos.TransportApiRequestMsg;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaEncoder;

/**
 * Created by ashvayka on 05.10.18.
 */
public class TransportApiRequestEncoder implements JnksIotKafkaEncoder<TransportApiRequestMsg> {
    @Override
    public byte[] encode(TransportApiRequestMsg value) {
        return value.toByteArray();
    }
}
