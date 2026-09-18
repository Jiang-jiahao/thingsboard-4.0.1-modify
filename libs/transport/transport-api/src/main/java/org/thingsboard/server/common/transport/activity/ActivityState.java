package org.thingsboard.server.common.transport.activity;

import lombok.Data;

@Data
public class ActivityState<Metadata> {

    private volatile long lastRecordedTime;
    private volatile Metadata metadata;

}
