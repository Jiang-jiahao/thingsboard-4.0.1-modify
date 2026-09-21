package com.jnks.iot.server.queue;

import com.google.protobuf.InvalidProtocolBufferException;

public interface JnksIotQueueMsgDecoder<T extends JnksIotQueueMsg> {

    T decode(JnksIotQueueMsg msg) throws InvalidProtocolBufferException;
}
