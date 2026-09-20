package com.jnks.iot.server.edqs.query.processor;

import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.permission.QueryContext;
import com.jnks.iot.server.common.data.query.EntityViewTypeFilter;
import com.jnks.iot.server.edqs.query.EdqsQuery;
import com.jnks.iot.server.edqs.repo.TenantRepo;

import java.util.List;

public class EntityViewTypeQueryProcessor extends AbstractEntityProfileNameQueryProcessor<EntityViewTypeFilter> {

    public EntityViewTypeQueryProcessor(TenantRepo repo, QueryContext ctx, EdqsQuery query) {
        super(repo, ctx, query, (EntityViewTypeFilter) query.getEntityFilter(), EntityType.ENTITY_VIEW);
    }

    @Override
    protected String getEntityNameFilter(EntityViewTypeFilter filter) {
        return filter.getEntityViewNameFilter();
    }

    @Override
    protected List<String> getProfileNames(EntityViewTypeFilter filter) {
        return filter.getEntityViewTypes();
    }

}
