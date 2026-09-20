package com.jnks.iot.server.common.data.kv;

import com.jnks.iot.server.common.data.HasVersion;

/**
 * @author Andrew Shvayka
 */
public interface AttributeKvEntry extends KvEntry, HasVersion {

    long getLastUpdateTs();

}
