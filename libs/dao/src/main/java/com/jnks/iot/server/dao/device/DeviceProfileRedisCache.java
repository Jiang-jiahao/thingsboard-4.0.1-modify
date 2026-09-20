package com.jnks.iot.server.dao.device;

import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CacheSpecsMap;
import com.jnks.iot.server.cache.TBRedisCacheConfiguration;
import com.jnks.iot.server.cache.TbRedisSerializer;
import com.jnks.iot.server.cache.VersionedRedisTbCache;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.util.ProtoUtils;
import com.jnks.iot.server.gen.transport.TransportProtos;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "redis")
@Service("DeviceProfileCache")
public class DeviceProfileRedisCache extends VersionedRedisTbCache<DeviceProfileCacheKey, DeviceProfile> {

    public DeviceProfileRedisCache(TBRedisCacheConfiguration configuration, CacheSpecsMap cacheSpecsMap, RedisConnectionFactory connectionFactory) {
        super(CacheConstants.DEVICE_PROFILE_CACHE, cacheSpecsMap, connectionFactory, configuration, new TbRedisSerializer<DeviceProfileCacheKey, DeviceProfile>() {
            @Override
            public byte[] serialize(DeviceProfile deviceProfile) throws SerializationException {
                return ProtoUtils.toProto(deviceProfile).toByteArray();
            }

            @Override
            public DeviceProfile deserialize(DeviceProfileCacheKey key, byte[] bytes) throws SerializationException {
                try {
                    return ProtoUtils.fromProto(TransportProtos.DeviceProfileProto.parseFrom(bytes));
                } catch (InvalidProtocolBufferException e) {
                    throw new SerializationException(e.getMessage());
                }
            }
        });
    }

}
