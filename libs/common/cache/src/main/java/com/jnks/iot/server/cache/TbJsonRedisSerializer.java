package com.jnks.iot.server.cache;

import org.springframework.data.redis.serializer.SerializationException;
import com.jnks.iot.common.util.JacksonUtil;

public class TbJsonRedisSerializer<K, V> implements TbRedisSerializer<K, V> {

    private final Class<V> clazz;

    public TbJsonRedisSerializer(Class<V> clazz) {
        this.clazz = clazz;
    }

    @Override
    public byte[] serialize(V v) throws SerializationException {
        return JacksonUtil.writeValueAsBytes(v);
    }

    @Override
    public V deserialize(K key, byte[] bytes) throws SerializationException {
        return JacksonUtil.fromBytes(bytes, clazz);
    }
}
