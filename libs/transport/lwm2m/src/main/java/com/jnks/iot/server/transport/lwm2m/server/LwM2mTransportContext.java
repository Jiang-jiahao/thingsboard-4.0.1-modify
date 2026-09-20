package com.jnks.iot.server.transport.lwm2m.server;

import lombok.Getter;
import lombok.Setter;
import org.eclipse.leshan.server.LeshanServer;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import com.jnks.iot.server.common.transport.TransportContext;

@Component
@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.lwm2m.enabled:false}'=='true'")
public class LwM2mTransportContext extends TransportContext {

    @Getter @Setter
    private LeshanServer server;

}
