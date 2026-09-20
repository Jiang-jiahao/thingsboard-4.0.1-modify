package com.jnks.iot.server.service.session;

import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CacheSpecsMap;
import com.jnks.iot.server.cache.RedisTbTransactionalCache;
import com.jnks.iot.server.cache.TBRedisCacheConfiguration;
import com.jnks.iot.server.cache.TbRedisSerializer;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.gen.transport.TransportProtos;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "redis")
@Service("SessionCache")
public class SessionRedisCache extends RedisTbTransactionalCache<DeviceId, TransportProtos.DeviceSessionsCacheEntry> {

    public SessionRedisCache(TBRedisCacheConfiguration configuration, CacheSpecsMap cacheSpecsMap, RedisConnectionFactory connectionFactory) {
        super(CacheConstants.SESSIONS_CACHE, cacheSpecsMap, connectionFactory, configuration, new TbRedisSerializer<>() {
            @Override
            public byte[] serialize(TransportProtos.DeviceSessionsCacheEntry deviceSessionsCacheEntry) throws SerializationException {
                return deviceSessionsCacheEntry.toByteArray();
            }

            @Override
            public TransportProtos.DeviceSessionsCacheEntry deserialize(DeviceId key, byte[] bytes) throws SerializationException {
                try {
                    return TransportProtos.DeviceSessionsCacheEntry.parseFrom(bytes);
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException("Failed to deserialize session cache entry");
                }
            }
        });
    }
}
