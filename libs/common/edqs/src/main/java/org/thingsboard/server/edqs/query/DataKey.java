package org.thingsboard.server.edqs.query;

import org.thingsboard.server.common.data.query.EntityKeyType;

public record DataKey(EntityKeyType type, String key, Integer keyId) {

}
