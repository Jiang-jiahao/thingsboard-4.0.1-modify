package com.jnks.iot.server.common.data.kv;

import lombok.Data;

@Data
public class TsKvEntryAggWrapper {

    private final TsKvEntry entry;
    private final long lastEntryTs;

}
