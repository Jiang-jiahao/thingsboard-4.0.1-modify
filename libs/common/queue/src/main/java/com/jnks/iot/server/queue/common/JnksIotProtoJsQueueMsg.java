package com.jnks.iot.server.queue.common;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.jnks.iot.server.queue.JnksIotQueueMsgHeaders;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class JnksIotProtoJsQueueMsg<T extends com.google.protobuf.GeneratedMessageV3> extends JnksIotProtoQueueMsg<T> {

    public JnksIotProtoJsQueueMsg(UUID key, T value) {
        super(key, value);
    }

    public JnksIotProtoJsQueueMsg(UUID key, T value, JnksIotQueueMsgHeaders headers) {
        super(key, value, headers);
    }

    @Override
    public byte[] getData() {
        try {
            return JsonFormat.printer().print(value).getBytes(StandardCharsets.UTF_8);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
