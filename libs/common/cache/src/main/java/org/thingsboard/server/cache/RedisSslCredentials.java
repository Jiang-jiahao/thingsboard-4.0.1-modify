package org.thingsboard.server.cache;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "redis.ssl.credentials")
@Data
public class RedisSslCredentials {

    private String certFile;

    private String userCertFile;

    private String userKeyFile;
}
