package com.jnks.iot.server.service.subscription;

import lombok.Data;
import com.jnks.iot.server.common.data.kv.ReadTsKvQuery;
import com.jnks.iot.server.service.ws.telemetry.cmd.v2.AggKey;

@Data
public class ReadTsKvQueryInfo {

    private final AggKey key;
    private final ReadTsKvQuery query;
    private final boolean previous;

}
