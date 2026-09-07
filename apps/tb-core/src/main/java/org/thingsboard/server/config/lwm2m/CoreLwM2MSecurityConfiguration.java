/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.config.lwm2m;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.thingsboard.server.common.transport.config.ssl.SslCredentials;
import org.thingsboard.server.common.transport.config.ssl.SslCredentialsConfig;
import org.thingsboard.server.common.transport.lwm2m.LwM2MSecureServerConfig;

/**
 * Standalone Core 没有 LwM2M 传输模块时，用 yml 组装服务器/引导服务器连接信息，供设备配置页展示。
 * Monolith 已有 {@code lwM2MTransportServerConfig} 时不会创建这些 bean。
 */
@Configuration
@ConditionalOnExpression("'${transport.lwm2m.enabled:false}'=='true'")
public class CoreLwM2MSecurityConfiguration {

    @Value("${transport.lwm2m.server.id:}")
    private Integer serverId;

    @Value("${transport.lwm2m.server.bind_address:}")
    private String serverHost;

    @Value("${transport.lwm2m.server.bind_port:}")
    private Integer serverPort;

    @Value("${transport.lwm2m.server.security.bind_address:}")
    private String serverSecureHost;

    @Value("${transport.lwm2m.server.security.bind_port:}")
    private Integer serverSecurePort;

    @Value("${transport.lwm2m.bootstrap.id:}")
    private Integer bootstrapId;

    @Value("${transport.lwm2m.bootstrap.bind_address:}")
    private String bootstrapHost;

    @Value("${transport.lwm2m.bootstrap.bind_port:}")
    private Integer bootstrapPort;

    @Value("${transport.lwm2m.bootstrap.security.bind_address:}")
    private String bootstrapSecureHost;

    @Value("${transport.lwm2m.bootstrap.security.bind_port:}")
    private Integer bootstrapSecurePort;

    @Bean(name = "lwM2MTransportServerConfig")
    @ConditionalOnMissingBean(name = "lwM2MTransportServerConfig")
    public LwM2MSecureServerConfig lwM2MTransportServerConfig(Environment environment) {
        return new SimpleLwM2MSecureServerConfig(
                serverId, serverHost, serverPort, serverSecureHost, serverSecurePort,
                bindCredentials(environment, "transport.lwm2m.server.security.credentials",
                        "LWM2M Server DTLS Credentials", false));
    }

    @Bean(name = "lwM2MTransportBootstrapConfig")
    @ConditionalOnMissingBean(name = "lwM2MTransportBootstrapConfig")
    public LwM2MSecureServerConfig lwM2MTransportBootstrapConfig(Environment environment) {
        return new SimpleLwM2MSecureServerConfig(
                bootstrapId, bootstrapHost, bootstrapPort, bootstrapSecureHost, bootstrapSecurePort,
                bindCredentials(environment, "transport.lwm2m.bootstrap.security.credentials",
                        "LWM2M Bootstrap DTLS Credentials", false));
    }

    private static SslCredentials bindCredentials(Environment environment, String prefix, String name, boolean trustsOnly) {
        SslCredentialsConfig credentialsConfig = new SslCredentialsConfig(name, trustsOnly);
        Binder.get(environment).bind(prefix, Bindable.ofInstance(credentialsConfig));
        credentialsConfig.init();
        return credentialsConfig.getCredentials();
    }

    @RequiredArgsConstructor
    private static final class SimpleLwM2MSecureServerConfig implements LwM2MSecureServerConfig {
        private final Integer id;
        private final String host;
        private final Integer port;
        private final String secureHost;
        private final Integer securePort;
        private final SslCredentials sslCredentials;

        @Override
        public Integer getId() {
            return id;
        }

        @Override
        public String getHost() {
            return host;
        }

        @Override
        public Integer getPort() {
            return port;
        }

        @Override
        public String getSecureHost() {
            return secureHost;
        }

        @Override
        public Integer getSecurePort() {
            return securePort;
        }

        @Override
        public SslCredentials getSslCredentials() {
            return sslCredentials;
        }
    }
}
