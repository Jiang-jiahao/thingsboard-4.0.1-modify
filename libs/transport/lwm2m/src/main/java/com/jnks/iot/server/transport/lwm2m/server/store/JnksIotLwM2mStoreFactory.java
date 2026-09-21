package com.jnks.iot.server.transport.lwm2m.server.store;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.leshan.server.registration.RegistrationStore;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import com.jnks.iot.server.cache.TBRedisCacheConfiguration;
import com.jnks.iot.server.transport.lwm2m.config.LwM2MTransportServerConfig;
import com.jnks.iot.server.transport.lwm2m.secure.LwM2mCredentialsSecurityInfoValidator;
import com.jnks.iot.server.transport.lwm2m.server.LwM2mVersionedModelProvider;

import java.util.Optional;

@Slf4j
@Component
@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.lwm2m.enabled:false}'=='true'")
@RequiredArgsConstructor
public class JnksIotLwM2mStoreFactory {

    private final Optional<TBRedisCacheConfiguration> redisConfiguration;
    private final LwM2MTransportServerConfig config;
    private final LwM2mCredentialsSecurityInfoValidator validator;
    private final LwM2mVersionedModelProvider modelProvider;

    @Bean
    private RegistrationStore registrationStore() {
        return redisConfiguration.isPresent() ?
                new JnksIotLwM2mRedisRegistrationStore(config, getConnectionFactory(), modelProvider) :
                new JnksIotInMemoryRegistrationStore(config, config.getCleanPeriodInSec(), modelProvider);
    }

    @Bean
    private JnksIotMainSecurityStore securityStore() {
        return new JnksIotLwM2mSecurityStore(redisConfiguration.isPresent() ?
                new JnksIotLwM2mRedisSecurityStore(getConnectionFactory()) : new JnksIotInMemorySecurityStore(), validator);
    }

    @Bean
    private JnksIotLwM2MClientStore clientStore() {
        return redisConfiguration.isPresent() ? new JnksIotRedisLwM2MClientStore(getConnectionFactory()) : new JnksIotDummyLwM2MClientStore();
    }

    @Bean
    private JnksIotLwM2MModelConfigStore modelConfigStore() {
        return redisConfiguration.isPresent() ? new JnksIotRedisLwM2MModelConfigStore(getConnectionFactory()) : new JnksIotDummyLwM2MModelConfigStore();
    }

    @Bean
    private JnksIotLwM2MClientOtaInfoStore otaStore() {
        return redisConfiguration.isPresent() ? new JnksIotLwM2mRedisClientOtaInfoStore(getConnectionFactory()) : new JnksIotDummyLwM2MClientOtaInfoStore();
    }

    @Bean
    private JnksIotLwM2MDtlsSessionStore sessionStore() {
        return redisConfiguration.isPresent() ? new JnksIotLwM2MDtlsSessionRedisStore(getConnectionFactory()) : new JnksIotL2M2MDtlsSessionInMemoryStore();
    }

    private RedisConnectionFactory getConnectionFactory() {
        return redisConfiguration.get().redisConnectionFactory();
    }

}
