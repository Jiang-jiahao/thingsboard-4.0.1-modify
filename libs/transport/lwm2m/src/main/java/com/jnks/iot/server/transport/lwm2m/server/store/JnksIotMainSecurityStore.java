package com.jnks.iot.server.transport.lwm2m.server.store;

import org.eclipse.leshan.server.security.NonUniqueSecurityInfoException;
import com.jnks.iot.server.transport.lwm2m.secure.JnksIotLwM2MSecurityInfo;

public interface JnksIotMainSecurityStore extends JnksIotSecurityStore {

    void putX509(JnksIotLwM2MSecurityInfo jnksIotSecurityInfo) throws NonUniqueSecurityInfoException;

    void registerX509(String endpoint, String registrationId);

    void remove(String endpoint, String registrationId);

}
