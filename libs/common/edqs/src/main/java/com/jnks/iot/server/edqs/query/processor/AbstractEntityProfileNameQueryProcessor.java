package com.jnks.iot.server.edqs.query.processor;

import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.permission.QueryContext;
import com.jnks.iot.server.common.data.query.EntityFilter;
import com.jnks.iot.server.edqs.data.EntityData;
import com.jnks.iot.server.edqs.query.EdqsQuery;
import com.jnks.iot.server.edqs.repo.TenantRepo;
import com.jnks.iot.server.edqs.util.RepositoryUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public abstract class AbstractEntityProfileNameQueryProcessor<T extends EntityFilter> extends AbstractSimpleQueryProcessor<T> {

    private final Set<String> entityProfileNames;
    private final Pattern pattern;

    public AbstractEntityProfileNameQueryProcessor(TenantRepo repo, QueryContext ctx, EdqsQuery query, T filter, EntityType entityType) {
        super(repo, ctx, query, filter, entityType);
        entityProfileNames = new HashSet<>(getProfileNames(this.filter));
        pattern = RepositoryUtils.toSqlLikePattern(getEntityNameFilter(filter));
    }

    protected abstract String getEntityNameFilter(T filter);

    protected abstract List<String> getProfileNames(T filter);

    @Override
    protected boolean matches(EntityData<?> ed) {
        return super.matches(ed) && entityProfileNames.contains(ed.getFields().getType())
                && (pattern == null || pattern.matcher(ed.getFields().getName()).matches());
    }

}
