package com.jnks.iot.server.dao.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.kv.KvEntry;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.exception.IncorrectParameterException;
import com.jnks.iot.server.dao.service.NoXssValidator;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class KvUtils {

    private static final Cache<String, Boolean> validatedKeys;

    static {
        validatedKeys = Caffeine.newBuilder()
                .weakKeys()
                .expireAfterAccess(24, TimeUnit.HOURS)
                .maximumSize(100000).build();
    }

    public static void validate(List<? extends KvEntry> tsKvEntries, boolean valueNoXssValidation) {
        tsKvEntries.forEach(tsKvEntry -> validate(tsKvEntry, valueNoXssValidation));
    }

    public static void validate(KvEntry tsKvEntry, boolean valueNoXssValidation) {
        if (tsKvEntry == null) {
            throw new IncorrectParameterException("Key value entry can't be null");
        }

        String key = tsKvEntry.getKey();

        if (StringUtils.isBlank(key)) {
            throw new DataValidationException("Key can't be null or empty");
        }

        if (key.length() > 255) {
            throw new DataValidationException("Validation error: key length must be equal or less than 255");
        }

        if (validatedKeys.getIfPresent(key) == null) {
            if (!NoXssValidator.isValid(key)) {
                throw new DataValidationException("Validation error: key is malformed");
            }
            validatedKeys.put(key, Boolean.TRUE);
        }

        if (valueNoXssValidation) {
            Object value = tsKvEntry.getValue();
            if (value instanceof CharSequence || value instanceof JsonNode) {
                if (!NoXssValidator.isValid(value.toString())) {
                    throw new DataValidationException("Validation error: value is malformed");
                }
            }
        }
    }
}
