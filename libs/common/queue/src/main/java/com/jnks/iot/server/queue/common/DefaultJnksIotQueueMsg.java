package com.jnks.iot.server.queue.common;

import lombok.Data;
import com.jnks.iot.server.queue.JnksIotQueueMsg;

import java.util.UUID;

@Data
public class DefaultJnksIotQueueMsg implements JnksIotQueueMsg {
    private final UUID key;
    private final byte[] data;
    private final DefaultJnksIotQueueMsgHeaders headers;

    public DefaultJnksIotQueueMsg(JnksIotQueueMsg msg) {
        this.key = msg.getKey();
        this.data = msg.getData();
        DefaultJnksIotQueueMsgHeaders headers = new DefaultJnksIotQueueMsgHeaders();
        msg.getHeaders().getData().forEach(headers::put);
        this.headers = headers;
    }

}
