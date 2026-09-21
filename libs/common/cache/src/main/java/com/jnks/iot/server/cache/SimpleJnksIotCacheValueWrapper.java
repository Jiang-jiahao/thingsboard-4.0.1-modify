package com.jnks.iot.server.cache;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.springframework.cache.Cache;

@ToString
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class SimpleJnksIotCacheValueWrapper<T> implements JnksIotCacheValueWrapper<T> {

    private final T value;

    @Override
    public T get() {
        return value;
    }

    public static <T> SimpleJnksIotCacheValueWrapper<T> empty() {
        return new SimpleJnksIotCacheValueWrapper<>(null);
    }

    public static <T> SimpleJnksIotCacheValueWrapper<T> wrap(T value) {
        return new SimpleJnksIotCacheValueWrapper<>(value);
    }

    @SuppressWarnings("unchecked")
    public static <T> SimpleJnksIotCacheValueWrapper<T> wrap(Cache.ValueWrapper source) {
        return source == null ? null : new SimpleJnksIotCacheValueWrapper<>((T) source.get());
    }
}
