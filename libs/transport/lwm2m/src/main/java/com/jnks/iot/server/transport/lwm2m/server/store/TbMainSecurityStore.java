package com.jnks.iot.server.transport.lwm2m.server.store;

import org.eclipse.leshan.server.security.NonUniqueSecurityInfoException;
import com.jnks.iot.server.transport.lwm2m.secure.TbLwM2MSecurityInfo;

public interface TbMainSecurityStore extends TbSecurityStore {

    void putX509(TbLwM2MSecurityInfo tbSecurityInfo) throws NonUniqueSecurityInfoException;

    void registerX509(String endpoint, String registrationId);

    void remove(String endpoint, String registrationId);

}
