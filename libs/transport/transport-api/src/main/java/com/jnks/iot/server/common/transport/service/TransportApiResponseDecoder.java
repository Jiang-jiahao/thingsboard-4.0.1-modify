package com.jnks.iot.server.common.transport.service;

import com.jnks.iot.server.gen.transport.TransportProtos.TransportApiResponseMsg;
import com.jnks.iot.server.queue.JnksIotQueueMsg;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaDecoder;

import java.io.IOException;

/**
 * Created by ashvayka on 05.10.18.
 */
public class TransportApiResponseDecoder implements JnksIotKafkaDecoder<TransportApiResponseMsg> {

    @Override
    public TransportApiResponseMsg decode(JnksIotQueueMsg msg) throws IOException {
        return TransportApiResponseMsg.parseFrom(msg.getData());
    }
}
