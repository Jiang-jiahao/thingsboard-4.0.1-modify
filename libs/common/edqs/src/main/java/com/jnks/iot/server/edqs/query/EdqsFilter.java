package com.jnks.iot.server.edqs.query;

import com.jnks.iot.server.common.data.query.EntityKeyValueType;
import com.jnks.iot.server.common.data.query.KeyFilterPredicate;

public record EdqsFilter(DataKey key, EntityKeyValueType valueType, KeyFilterPredicate predicate) {

}
