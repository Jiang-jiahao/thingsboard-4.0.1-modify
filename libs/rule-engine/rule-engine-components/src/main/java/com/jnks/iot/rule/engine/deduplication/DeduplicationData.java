package com.jnks.iot.rule.engine.deduplication;

import lombok.Data;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.LinkedList;
import java.util.List;

@Data
public class DeduplicationData {

    private final List<JnksIotMsg> msgList;
    private boolean tickScheduled;

    public DeduplicationData() {
        msgList = new LinkedList<>();
    }

    public int size() {
        return msgList.size();
    }

    public void add(JnksIotMsg msg) {
        msgList.add(msg);
    }

    public boolean isEmpty() {
        return msgList.isEmpty();
    }
}
