package org.thingsboard.server.service.ws.telemetry.cmd.v1;

import lombok.NoArgsConstructor;
import org.thingsboard.server.service.ws.WsCmdType;

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
