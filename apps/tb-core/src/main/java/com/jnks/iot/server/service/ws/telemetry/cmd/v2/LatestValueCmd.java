package com.jnks.iot.server.service.ws.telemetry.cmd.v2;

import lombok.Data;
import com.jnks.iot.server.common.data.query.EntityKey;

import java.util.List;

@Data
public class LatestValueCmd {

    private List<EntityKey> keys;

}
