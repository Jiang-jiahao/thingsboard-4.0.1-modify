package com.jnks.iot.server.queue;

import java.util.UUID;

public interface JnksIotQueueMsg {

    UUID getKey();

    JnksIotQueueMsgHeaders getHeaders();

    byte[] getData();
}
