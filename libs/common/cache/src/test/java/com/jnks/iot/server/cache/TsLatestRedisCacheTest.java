package com.jnks.iot.server.cache;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.security.MessageDigest;

import static org.assertj.core.api.Assertions.assertThat;

class VersionedRedisJnksIotCacheTest {

    @Test
    void testUpsertTsLatestLUAScriptHash() {
        assertThat(getSHA1(VersionedRedisJnksIotCache.SET_VERSIONED_VALUE_LUA_SCRIPT)).isEqualTo(new String(VersionedRedisJnksIotCache.SET_VERSIONED_VALUE_SHA));
    }

    @SneakyThrows
    String getSHA1(byte[] script) {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] hash = md.digest(script);

        StringBuilder sb = new StringBuilder();
        for (byte b : hash) {
            sb.append(String.format("%02x", b));
        }

        return sb.toString();
    }

}