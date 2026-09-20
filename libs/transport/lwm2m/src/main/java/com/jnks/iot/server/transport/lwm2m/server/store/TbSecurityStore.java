package com.jnks.iot.server.transport.lwm2m.server.store;

import org.eclipse.leshan.server.security.SecurityStore;
import com.jnks.iot.server.transport.lwm2m.secure.TbLwM2MSecurityInfo;

public interface TbSecurityStore extends SecurityStore {

    TbLwM2MSecurityInfo getTbLwM2MSecurityInfoByEndpoint(String endpoint);

}
