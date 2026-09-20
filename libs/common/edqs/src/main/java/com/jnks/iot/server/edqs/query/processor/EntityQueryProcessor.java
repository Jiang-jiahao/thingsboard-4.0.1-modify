package com.jnks.iot.server.edqs.query.processor;

import com.jnks.iot.server.edqs.query.SortableEntityData;

import java.util.List;

public interface EntityQueryProcessor {

    List<SortableEntityData> processQuery();

    long count();

}
