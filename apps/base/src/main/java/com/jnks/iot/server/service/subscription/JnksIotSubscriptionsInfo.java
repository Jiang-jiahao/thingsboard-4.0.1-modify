package com.jnks.iot.server.service.subscription;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.HashSet;
import java.util.Set;

/**
 * Information about the local websocket subscriptions.
 */
@RequiredArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(exclude = {"seqNumber"})
@ToString
public class JnksIotSubscriptionsInfo {

    protected boolean notifications;
    protected boolean alarms;
    protected boolean tsAllKeys;
    protected Set<String> tsKeys;
    protected boolean attrAllKeys;
    protected Set<String> attrKeys;
    protected int seqNumber;

    public boolean isEmpty() {
        return !notifications && !alarms && !tsAllKeys && !attrAllKeys && tsKeys == null && attrKeys == null;
    }

    protected JnksIotSubscriptionsInfo copy() {
        return copy(0);
    }

    protected JnksIotSubscriptionsInfo copy(int seqNumber) {
        return new JnksIotSubscriptionsInfo(notifications, alarms, tsAllKeys, tsKeys != null ? new HashSet<>(tsKeys) : null, attrAllKeys, attrKeys != null ? new HashSet<>(attrKeys) : null, seqNumber);
    }

}
