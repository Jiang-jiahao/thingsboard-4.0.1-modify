package com.jnks.iot.server.cache.device;

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
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.util.ProtoUtils;
import com.jnks.iot.server.gen.transport.TransportProtos;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "redis")
@Service("DeviceCache")
public class DeviceRedisCache extends VersionedRedisJnksIotCache<DeviceCacheKey, Device> {

    public DeviceRedisCache(TBRedisCacheConfiguration configuration, CacheSpecsMap cacheSpecsMap, RedisConnectionFactory connectionFactory) {
        super(CacheConstants.DEVICE_CACHE, cacheSpecsMap, connectionFactory, configuration, new JnksIotRedisSerializer<>() {

            @Override
            public byte[] serialize(Device device) throws SerializationException {
                return ProtoUtils.toProto(device).toByteArray();
            }

            @Override
            public Device deserialize(DeviceCacheKey key, byte[] bytes) throws SerializationException {
                try {
                    return ProtoUtils.fromProto(TransportProtos.DeviceProto.parseFrom(bytes));
                } catch (InvalidProtocolBufferException e) {
                    throw new SerializationException(e.getMessage());
                }
            }
        });
    }
}
