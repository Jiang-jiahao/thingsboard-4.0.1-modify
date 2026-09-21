package com.jnks.iot.server.cache;

import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;

public class JnksIotJavaRedisSerializer<K, V> implements JnksIotRedisSerializer<K, V> {

    final RedisSerializer<Object> serializer = RedisSerializer.java();

    @Override
    public byte[] serialize(V value) throws SerializationException {
        return serializer.serialize(value);
    }

    @Override
    public V deserialize(K key, byte[] bytes) throws SerializationException {
        return (V) serializer.deserialize(bytes);
    }

}
