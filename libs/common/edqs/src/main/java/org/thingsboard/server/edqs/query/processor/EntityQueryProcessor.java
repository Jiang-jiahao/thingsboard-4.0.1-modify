package org.thingsboard.server.edqs.query.processor;

import org.thingsboard.server.edqs.query.SortableEntityData;

import java.util.List;

public interface EntityQueryProcessor {

    List<SortableEntityData> processQuery();

    long count();

}
