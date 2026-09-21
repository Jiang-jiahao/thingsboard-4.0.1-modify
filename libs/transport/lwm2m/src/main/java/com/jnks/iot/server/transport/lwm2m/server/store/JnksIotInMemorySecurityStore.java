package com.jnks.iot.server.transport.lwm2m.server.store;

import org.eclipse.leshan.core.SecurityMode;
import org.eclipse.leshan.core.peer.OscoreIdentity;
import org.eclipse.leshan.server.security.NonUniqueSecurityInfoException;
import org.eclipse.leshan.server.security.SecurityInfo;
import com.jnks.iot.server.transport.lwm2m.secure.JnksIotLwM2MSecurityInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class JnksIotInMemorySecurityStore implements JnksIotEditableSecurityStore {
    // lock for the two maps
    protected final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    protected final Lock readLock = readWriteLock.readLock();
    protected final Lock writeLock = readWriteLock.writeLock();

    // by client end-point
    protected Map<String, JnksIotLwM2MSecurityInfo> securityByEp = new HashMap<>();

    // by PSK identity
    protected Map<String, JnksIotLwM2MSecurityInfo> securityByIdentity = new HashMap<>();

    public JnksIotInMemorySecurityStore() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SecurityInfo getByEndpoint(String endpoint) {
        readLock.lock();
        try {
            JnksIotLwM2MSecurityInfo securityInfo = securityByEp.get(endpoint);
            if (securityInfo != null ) {
                if (SecurityMode.NO_SEC.equals(securityInfo.getSecurityMode())) {
                    return SecurityInfo.newPreSharedKeyInfo(SecurityMode.NO_SEC.toString(), SecurityMode.NO_SEC.toString(),
                            SecurityMode.NO_SEC.toString().getBytes());
                } else {
                    return securityInfo.getSecurityInfo();
                }
            }
            else {
                return null;
            }
        } finally {
            readLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SecurityInfo getByIdentity(String identity) {
        readLock.lock();
        try {
            JnksIotLwM2MSecurityInfo securityInfo = securityByIdentity.get(identity);
            if (securityInfo != null) {
                return securityInfo.getSecurityInfo();
            } else {
                return null;
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public SecurityInfo getByOscoreIdentity(OscoreIdentity oscoreIdentity) {
        return null;
    }

    @Override
    public void put(JnksIotLwM2MSecurityInfo jnksIotSecurityInfo) throws NonUniqueSecurityInfoException {
        writeLock.lock();
        try {
            String identity = null;
            if (jnksIotSecurityInfo.getSecurityInfo() != null) {
                identity = jnksIotSecurityInfo.getSecurityInfo().getPskIdentity();
                if (identity != null) {
                    JnksIotLwM2MSecurityInfo infoByIdentity = securityByIdentity.get(identity);
                    if (infoByIdentity != null && !jnksIotSecurityInfo.getSecurityInfo().getEndpoint().equals(infoByIdentity.getEndpoint())) {
                        throw new NonUniqueSecurityInfoException("PSK Identity " + identity + " is already used");
                    }
                    securityByIdentity.put(jnksIotSecurityInfo.getSecurityInfo().getPskIdentity(), jnksIotSecurityInfo);
                }
            }

            JnksIotLwM2MSecurityInfo previous = securityByEp.put(jnksIotSecurityInfo.getEndpoint(), jnksIotSecurityInfo);
            if (previous != null && previous.getSecurityInfo() != null) {
                String previousIdentity = previous.getSecurityInfo().getPskIdentity();
                if (previousIdentity != null && !previousIdentity.equals(identity)) {
                    securityByIdentity.remove(previousIdentity);
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void remove(String endpoint) {
        writeLock.lock();
        try {
            JnksIotLwM2MSecurityInfo securityInfo = securityByEp.remove(endpoint);
            if (securityInfo != null && securityInfo.getSecurityInfo() != null && securityInfo.getSecurityInfo().getPskIdentity() != null) {
                securityByIdentity.remove(securityInfo.getSecurityInfo().getPskIdentity());
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public JnksIotLwM2MSecurityInfo getJnksIotLwM2MSecurityInfoByEndpoint(String endpoint) {
        readLock.lock();
        try {
            return securityByEp.get(endpoint);
        } finally {
            readLock.unlock();
        }
    }

}
