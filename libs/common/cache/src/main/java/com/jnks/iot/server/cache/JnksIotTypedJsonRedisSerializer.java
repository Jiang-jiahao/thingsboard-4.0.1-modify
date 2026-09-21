package com.jnks.iot.server.cache;

import com.fasterxml.jackson.core.type.TypeReference;
import org.springframework.data.redis.serializer.SerializationException;
import com.jnks.iot.common.util.JacksonUtil;

public class JnksIotTypedJsonRedisSerializer<K, V> implements JnksIotRedisSerializer<K, V> {

    private final TypeReference<V> valueTypeRef;

    public JnksIotTypedJsonRedisSerializer(TypeReference<V> valueTypeRef) {
        this.valueTypeRef = valueTypeRef;
    }

    @Override
    public byte[] serialize(V v) throws SerializationException {
        return JacksonUtil.writeValueAsBytes(v);
    }

    @Override
    public V deserialize(K key, byte[] bytes) throws SerializationException {
        return JacksonUtil.fromBytes(bytes, valueTypeRef);
    }
}
