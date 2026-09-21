package com.jnks.iot.server.queue;

import java.util.Map;

public interface JnksIotQueueMsgHeaders {

    byte[] put(String key, byte[] value);

    byte[] get(String key);

    Map<String, byte[]> getData();
}
