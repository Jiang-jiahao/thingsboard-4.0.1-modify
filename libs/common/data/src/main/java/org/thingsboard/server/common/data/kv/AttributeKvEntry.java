package org.thingsboard.server.common.data.kv;

import org.thingsboard.server.common.data.HasVersion;

/**
 * @author Andrew Shvayka
 */
public interface AttributeKvEntry extends KvEntry, HasVersion {

    long getLastUpdateTs();

}
