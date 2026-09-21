package com.jnks.iot.server.service.script;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.jnks.iot.server.gen.js.JsInvokeProtos;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaEncoder;

import java.nio.charset.StandardCharsets;

/**
 * Created by ashvayka on 25.09.18.
 */
public class RemoteJsRequestEncoder implements JnksIotKafkaEncoder<JnksIotProtoQueueMsg<JsInvokeProtos.RemoteJsRequest>> {
    @Override
    public byte[] encode(JnksIotProtoQueueMsg<JsInvokeProtos.RemoteJsRequest> value) {
        try {
            return JsonFormat.printer().print(value.getValue()).getBytes(StandardCharsets.UTF_8);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
