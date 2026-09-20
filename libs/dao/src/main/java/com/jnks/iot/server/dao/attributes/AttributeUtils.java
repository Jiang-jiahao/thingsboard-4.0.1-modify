package com.jnks.iot.server.dao.attributes;

import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;
import com.jnks.iot.server.dao.exception.IncorrectParameterException;
import com.jnks.iot.server.dao.service.Validator;
import com.jnks.iot.server.dao.util.KvUtils;

import java.util.List;

public class AttributeUtils {

    @Deprecated(since = "3.7.0")
    public static void validate(EntityId id, String scope) {
        Validator.validateId(id.getId(), uuid -> "Incorrect id " + uuid);
        Validator.validateString(scope, sc -> "Incorrect scope " + sc);
    }

    public static void validate(EntityId id, AttributeScope scope) {
        Validator.validateId(id.getId(), uuid -> "Incorrect id " + uuid);
        Validator.checkNotNull(scope, "Incorrect scope " + scope);
    }

    public static void validate(List<AttributeKvEntry> kvEntries,  boolean valueNoXssValidation) {
        kvEntries.forEach(tsKvEntry -> validate(tsKvEntry, valueNoXssValidation));
    }

    public static void validate(AttributeKvEntry kvEntry, boolean valueNoXssValidation) {
        KvUtils.validate(kvEntry, valueNoXssValidation);
        if (kvEntry.getDataType() == null) {
            throw new IncorrectParameterException("Incorrect kvEntry. Data type can't be null");
        } else {
            Validator.validateString(kvEntry.getKey(), "Incorrect kvEntry. Key can't be empty");
            Validator.validatePositiveNumber(kvEntry.getLastUpdateTs(), "Incorrect last update ts. Ts should be positive");
        }
    }
}
