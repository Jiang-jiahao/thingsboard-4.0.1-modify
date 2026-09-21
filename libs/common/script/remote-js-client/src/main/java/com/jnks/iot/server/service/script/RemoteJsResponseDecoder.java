package com.jnks.iot.server.service.script;

import com.google.protobuf.util.JsonFormat;
import com.jnks.iot.server.gen.js.JsInvokeProtos;
import com.jnks.iot.server.queue.JnksIotQueueMsg;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaDecoder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Created by ashvayka on 25.09.18.
 */
public class RemoteJsResponseDecoder implements JnksIotKafkaDecoder<JnksIotProtoQueueMsg<JsInvokeProtos.RemoteJsResponse>> {

    @Override
    public JnksIotProtoQueueMsg<JsInvokeProtos.RemoteJsResponse> decode(JnksIotQueueMsg msg) throws IOException {
        JsInvokeProtos.RemoteJsResponse.Builder builder = JsInvokeProtos.RemoteJsResponse.newBuilder();
        JsonFormat.parser().ignoringUnknownFields().merge(new String(msg.getData(), StandardCharsets.UTF_8), builder);
        return new JnksIotProtoQueueMsg<>(msg.getKey(), builder.build(), msg.getHeaders());
    }
}
