package com.jnks.iot.server.common.msg;

import lombok.Data;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by ashvayka on 13.01.18.
 */
@Data
public final class JnksIotMsgMetaData implements Serializable {

    public static final JnksIotMsgMetaData EMPTY = new JnksIotMsgMetaData(0);

    private final Map<String, String> data;

    public JnksIotMsgMetaData() {
        this.data = new ConcurrentHashMap<>();
    }

    public JnksIotMsgMetaData(Map<String, String> data) {
        this.data = new ConcurrentHashMap<>();
        data.forEach(this::putValue);
    }

    /**
     * Internal constructor to create immutable JnksIotMsgMetaData.EMPTY
     * */
    private JnksIotMsgMetaData(int ignored) {
        this.data = Collections.emptyMap();
    }

    public String getValue(String key) {
        return this.data.get(key);
    }

    public void putValue(String key, String value) {
        if (key != null && value != null) {
            this.data.put(key, value);
        }
    }

    public Map<String, String> values() {
        return new HashMap<>(this.data);
    }

    public JnksIotMsgMetaData copy() {
        return new JnksIotMsgMetaData(this.data);
    }
}
