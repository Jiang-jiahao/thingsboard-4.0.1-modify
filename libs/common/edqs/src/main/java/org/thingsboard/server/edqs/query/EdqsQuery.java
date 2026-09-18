package org.thingsboard.server.edqs.query;

import lombok.Data;
import org.thingsboard.server.common.data.query.EntityFilter;

import java.util.List;

@Data
public abstract class EdqsQuery {

    private final EntityFilter entityFilter;
    private final boolean hasKeyFilters;
    private final List<EdqsFilter> keyFilters;

}
