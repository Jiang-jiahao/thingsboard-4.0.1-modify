package org.thingsboard.server.transport.lwm2m.config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.thingsboard.server.common.transport.config.ssl.SslCredentials;
import org.thingsboard.server.common.transport.config.ssl.SslCredentialsConfig;
import org.thingsboard.server.common.transport.lwm2m.LwM2MSecureServerConfig;

@Slf4j
@Component("lwM2MTransportBootstrapConfig")
public class LwM2MTransportBootstrapConfig implements LwM2MSecureServerConfig {

    @Getter
    @Value("${transport.lwm2m.bootstrap.id:}")
    private Integer id;

    @Getter
    @Value("${transport.lwm2m.bootstrap.bind_address:}")
    private String host;

    @Getter
    @Value("${transport.lwm2m.bootstrap.bind_port:}")
    private Integer port;

    @Getter
    @Value("${transport.lwm2m.bootstrap.security.bind_address:}")
    private String secureHost;

    @Getter
    @Value("${transport.lwm2m.bootstrap.security.bind_port:}")
    private Integer securePort;

    @Bean
    @ConfigurationProperties(prefix = "transport.lwm2m.bootstrap.security.credentials")
    public SslCredentialsConfig lwm2mBootstrapCredentials() {
        return new SslCredentialsConfig("LWM2M Bootstrap DTLS Credentials", false);
    }

    @Autowired
    @Qualifier("lwm2mBootstrapCredentials")
    private SslCredentialsConfig credentialsConfig;

    @Override
    public SslCredentials getSslCredentials() {
        return this.credentialsConfig.getCredentials();
    }
}
