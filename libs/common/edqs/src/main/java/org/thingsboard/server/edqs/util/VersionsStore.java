package org.thingsboard.server.edqs.util;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class VersionsStore {

    private final Cache<String, Long> versions = Caffeine.newBuilder()
            .expireAfterWrite(24, TimeUnit.HOURS)
            .build();

    public boolean isNew(String key, Long version) {
        AtomicBoolean isNew = new AtomicBoolean(false);
        versions.asMap().compute(key, (k, prevVersion) -> {
            if (prevVersion == null || prevVersion <= version) {
                isNew.set(true);
                return version;
            } else {
                log.info("[{}] Version {} is outdated, the latest is {}", key, version, prevVersion);
                return prevVersion;
            }
        });
        return isNew.get();
    }

}
