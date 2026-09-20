package com.jnks.iot.server.actors.tenant;

import lombok.AllArgsConstructor;
import lombok.Data;
import com.jnks.iot.server.common.msg.tools.TbRateLimits;

@Data
@AllArgsConstructor
public class DebugTbRateLimits {

    private TbRateLimits tbRateLimits;
    private boolean ruleChainEventSaved;

}
