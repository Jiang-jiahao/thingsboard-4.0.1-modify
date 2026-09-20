package com.jnks.iot.server.edqs.query;

import com.jnks.iot.server.common.data.query.EntityKeyType;

public record DataKey(EntityKeyType type, String key, Integer keyId) {

}
