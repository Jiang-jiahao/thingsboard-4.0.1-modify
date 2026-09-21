package com.jnks.iot.server.dao.attributes;

import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CacheSpecsMap;
import com.jnks.iot.server.cache.TBRedisCacheConfiguration;
import com.jnks.iot.server.cache.JnksIotRedisSerializer;
import com.jnks.iot.server.cache.VersionedRedisJnksIotCache;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;
import com.jnks.iot.server.common.util.ProtoUtils;
import com.jnks.iot.server.gen.transport.TransportProtos.AttributeValueProto;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "redis")
@Service("AttributeCache")
public class AttributeRedisCache extends VersionedRedisJnksIotCache<AttributeCacheKey, AttributeKvEntry> {

    public AttributeRedisCache(TBRedisCacheConfiguration configuration, CacheSpecsMap cacheSpecsMap, RedisConnectionFactory connectionFactory) {
        super(CacheConstants.ATTRIBUTES_CACHE, cacheSpecsMap, connectionFactory, configuration, new JnksIotRedisSerializer<>() {
            @Override
            public byte[] serialize(AttributeKvEntry attributeKvEntry) throws SerializationException {
                return ProtoUtils.toProto(attributeKvEntry).toByteArray();
            }

            @Override
            public AttributeKvEntry deserialize(AttributeCacheKey key, byte[] bytes) throws SerializationException {
                try {
                    return ProtoUtils.fromProto(AttributeValueProto.parseFrom(bytes));
                } catch (InvalidProtocolBufferException e) {
                    throw new SerializationException(e.getMessage());
                }
            }
        });
    }

}
