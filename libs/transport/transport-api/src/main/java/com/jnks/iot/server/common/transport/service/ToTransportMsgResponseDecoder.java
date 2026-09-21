package com.jnks.iot.server.common.transport.service;

import com.jnks.iot.server.gen.transport.TransportProtos.ToTransportMsg;
import com.jnks.iot.server.queue.JnksIotQueueMsg;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaDecoder;

import java.io.IOException;

/**
 * Created by ashvayka on 05.10.18.
 */
public class ToTransportMsgResponseDecoder implements JnksIotKafkaDecoder<ToTransportMsg> {

    @Override
    public ToTransportMsg decode(JnksIotQueueMsg msg) throws IOException {
        return ToTransportMsg.parseFrom(msg.getData());
    }
}
