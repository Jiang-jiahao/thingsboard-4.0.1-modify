package org.thingsboard.server.common.transport.lwm2m;

import org.thingsboard.server.common.transport.config.ssl.SslCredentials;

public interface LwM2MSecureServerConfig {

    Integer getId();

    String getHost();

    Integer getPort();

    String getSecureHost();

    Integer getSecurePort();

    SslCredentials getSslCredentials();

}
