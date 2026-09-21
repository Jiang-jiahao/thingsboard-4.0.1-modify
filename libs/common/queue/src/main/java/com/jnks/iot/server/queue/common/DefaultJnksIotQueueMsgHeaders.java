package com.jnks.iot.server.queue.common;

import com.jnks.iot.server.queue.JnksIotQueueMsgHeaders;

import java.util.HashMap;
import java.util.Map;

public class DefaultJnksIotQueueMsgHeaders implements JnksIotQueueMsgHeaders {

    protected final Map<String, byte[]> data = new HashMap<>();

    @Override
    public byte[] put(String key, byte[] value) {
        return data.put(key, value);
    }

    @Override
    public byte[] get(String key) {
        return data.get(key);
    }

    @Override
    public Map<String, byte[]> getData() {
        return data;
    }
}
