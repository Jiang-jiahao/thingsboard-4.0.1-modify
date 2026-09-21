package com.jnks.iot.server.actors.tenant;

import lombok.AllArgsConstructor;
import lombok.Data;
import com.jnks.iot.server.common.msg.tools.JnksIotRateLimits;

@Data
@AllArgsConstructor
public class DebugJnksIotRateLimits {

    private JnksIotRateLimits jnksIotRateLimits;
    private boolean ruleChainEventSaved;

}
