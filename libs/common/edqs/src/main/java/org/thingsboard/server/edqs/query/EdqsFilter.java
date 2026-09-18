package org.thingsboard.server.edqs.query;

import org.thingsboard.server.common.data.query.EntityKeyValueType;
import org.thingsboard.server.common.data.query.KeyFilterPredicate;

public record EdqsFilter(DataKey key, EntityKeyValueType valueType, KeyFilterPredicate predicate) {

}
