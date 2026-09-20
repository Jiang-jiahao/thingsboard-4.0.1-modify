package com.jnks.iot.server.edqs.query.processor;

import com.jnks.iot.server.common.data.permission.QueryContext;
import com.jnks.iot.server.common.data.query.EntityTypeFilter;
import com.jnks.iot.server.edqs.data.EntityData;
import com.jnks.iot.server.edqs.query.EdqsQuery;
import com.jnks.iot.server.edqs.repo.TenantRepo;

public class EntityTypeQueryProcessor extends AbstractSimpleQueryProcessor<EntityTypeFilter> {

    public EntityTypeQueryProcessor(TenantRepo repo, QueryContext ctx, EdqsQuery query) {
        super(repo, ctx, query, (EntityTypeFilter) query.getEntityFilter(), ((EntityTypeFilter) query.getEntityFilter()).getEntityType());
    }

    @Override
    protected boolean matches(EntityData ed) {
        return super.matches(ed);
    }

}
