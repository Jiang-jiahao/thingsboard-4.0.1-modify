package org.thingsboard.server.common.data.plugin;

import java.io.Serializable;

/**
 * @author Andrew Shvayka
 */
public enum ComponentLifecycleEvent implements Serializable {
    // In sync with ComponentLifecycleEvent proto
    CREATED, STARTED, ACTIVATED, SUSPENDED, UPDATED, STOPPED, DELETED, FAILED, DEACTIVATED
}
