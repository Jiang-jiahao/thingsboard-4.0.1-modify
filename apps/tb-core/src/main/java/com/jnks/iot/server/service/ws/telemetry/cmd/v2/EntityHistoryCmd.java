package com.jnks.iot.server.service.ws.telemetry.cmd.v2;

import lombok.Data;
import com.jnks.iot.server.common.data.kv.Aggregation;
import com.jnks.iot.server.common.data.kv.IntervalType;

import java.util.List;

@Data
public class EntityHistoryCmd implements GetTsCmd {

    private List<String> keys;
    private long startTs;
    private long endTs;
    private IntervalType intervalType;
    private long interval;
    private String timeZoneId;
    private int limit;
    private Aggregation agg;
    private boolean fetchLatestPreviousPoint;

}
