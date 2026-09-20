package com.jnks.iot.server.edqs.query.processor;

import com.jnks.iot.server.common.data.permission.QueryContext;
import com.jnks.iot.server.common.data.query.EntityNameFilter;
import com.jnks.iot.server.edqs.data.EntityData;
import com.jnks.iot.server.edqs.query.EdqsQuery;
import com.jnks.iot.server.edqs.repo.TenantRepo;
import com.jnks.iot.server.edqs.util.RepositoryUtils;

import java.util.regex.Pattern;

public class EntityNameQueryProcessor extends AbstractSimpleQueryProcessor<EntityNameFilter> {

    private final Pattern pattern;

    public EntityNameQueryProcessor(TenantRepo repo, QueryContext ctx, EdqsQuery query) {
        super(repo, ctx, query, (EntityNameFilter) query.getEntityFilter(), ((EntityNameFilter) query.getEntityFilter()).getEntityType());
        pattern = RepositoryUtils.toSqlLikePattern(filter.getEntityNameFilter());
    }

    @Override
    protected boolean matches(EntityData ed) {
        return ed.getFields() != null && (pattern == null || pattern.matcher(ed.getFields().getName()).matches());
    }

}
