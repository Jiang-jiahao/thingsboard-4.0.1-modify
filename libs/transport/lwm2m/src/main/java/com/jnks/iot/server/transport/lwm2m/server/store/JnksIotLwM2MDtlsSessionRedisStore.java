package com.jnks.iot.server.transport.lwm2m.server.store;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import com.jnks.iot.server.common.data.JavaSerDesUtil;
import com.jnks.iot.server.transport.lwm2m.secure.JnksIotX509DtlsSessionInfo;

public class JnksIotLwM2MDtlsSessionRedisStore implements JnksIotLwM2MDtlsSessionStore {

    private static final String SESSION_EP = "SESSION#EP#";
    private final RedisConnectionFactory connectionFactory;

    public JnksIotLwM2MDtlsSessionRedisStore(RedisConnectionFactory redisConnectionFactory) {
        this.connectionFactory = redisConnectionFactory;
    }

    @Override
    public void put(String endpoint, JnksIotX509DtlsSessionInfo msg) {
        try (var c = connectionFactory.getConnection()) {
            var serializedMsg = JavaSerDesUtil.encode(msg);
            if (serializedMsg != null) {
                c.set(getKey(endpoint), serializedMsg);
            } else {
                throw new RuntimeException("Problem with serialization of message: " + msg);
            }
        }
    }

    @Override
    public JnksIotX509DtlsSessionInfo get(String endpoint) {
        try (var c = connectionFactory.getConnection()) {
            var data = c.get(getKey(endpoint));
            if (data != null) {
                return JavaSerDesUtil.decode(data);
            } else {
                return null;
            }
        }
    }

    @Override
    public void remove(String endpoint) {
        try (var c = connectionFactory.getConnection()) {
            c.del(getKey(endpoint));
        }
    }

    private byte[] getKey(String endpoint) {
        return (SESSION_EP + endpoint).getBytes();
    }
}
