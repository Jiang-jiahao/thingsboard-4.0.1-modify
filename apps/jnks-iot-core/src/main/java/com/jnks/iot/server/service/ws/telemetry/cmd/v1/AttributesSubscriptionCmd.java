package com.jnks.iot.server.service.ws.telemetry.cmd.v1;

import lombok.NoArgsConstructor;
import com.jnks.iot.server.service.ws.WsCmdType;

/**
 * @author Andrew Shvayka
 */
@NoArgsConstructor
public class AttributesSubscriptionCmd extends SubscriptionCmd {

    @Override
    public WsCmdType getType() {
        return WsCmdType.ATTRIBUTES;
    }

}
