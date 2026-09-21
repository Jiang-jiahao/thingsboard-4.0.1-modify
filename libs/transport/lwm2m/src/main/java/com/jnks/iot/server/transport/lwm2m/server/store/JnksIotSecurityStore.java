package com.jnks.iot.server.transport.lwm2m.server.store;

import org.eclipse.leshan.server.security.SecurityStore;
import com.jnks.iot.server.transport.lwm2m.secure.JnksIotLwM2MSecurityInfo;

public interface JnksIotSecurityStore extends SecurityStore {

    JnksIotLwM2MSecurityInfo getJnksIotLwM2MSecurityInfoByEndpoint(String endpoint);

}
