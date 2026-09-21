package com.jnks.iot.server.transport.lwm2m.server.store;

import org.eclipse.leshan.server.security.NonUniqueSecurityInfoException;
import com.jnks.iot.server.transport.lwm2m.secure.JnksIotLwM2MSecurityInfo;

public interface JnksIotEditableSecurityStore extends JnksIotSecurityStore {

    void put(JnksIotLwM2MSecurityInfo jnksIotSecurityInfo) throws NonUniqueSecurityInfoException;

    void remove(String endpoint);

}
