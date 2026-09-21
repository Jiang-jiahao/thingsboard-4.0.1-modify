package com.jnks.iot.server.queue.common;

import lombok.Data;
import com.jnks.iot.server.queue.JnksIotQueueMsg;
import com.jnks.iot.server.queue.JnksIotQueueMsgHeaders;

import java.util.UUID;

@Data
public class JnksIotProtoQueueMsg<T extends com.google.protobuf.GeneratedMessageV3> implements JnksIotQueueMsg {

    private final UUID key;
    protected final T value;
    private final JnksIotQueueMsgHeaders headers;

    public JnksIotProtoQueueMsg(UUID key, T value) {
        this(key, value, new DefaultJnksIotQueueMsgHeaders());
    }

    public JnksIotProtoQueueMsg(UUID key, T value, JnksIotQueueMsgHeaders headers) {
        this.key = key;
        this.value = value;
        this.headers = headers;
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
        return value != null ? value.toByteArray() : null;
    }

}
