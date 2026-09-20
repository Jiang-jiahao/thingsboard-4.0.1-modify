package com.jnks.iot.server.edqs.query.processor;

import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.permission.QueryContext;
import com.jnks.iot.server.common.data.query.EntitySearchQueryFilter;
import com.jnks.iot.server.common.data.relation.EntitySearchDirection;
import com.jnks.iot.server.edqs.data.EntityData;
import com.jnks.iot.server.edqs.data.RelationInfo;
import com.jnks.iot.server.edqs.query.EdqsQuery;
import com.jnks.iot.server.edqs.repo.TenantRepo;

import java.util.Set;
import java.util.UUID;

public abstract class AbstractEntitySearchQueryProcessor<T extends EntitySearchQueryFilter> extends AbstractRelationQueryProcessor<T> {


    public AbstractEntitySearchQueryProcessor(TenantRepo repo, QueryContext ctx, EdqsQuery query, T filter) {
        super(repo, ctx, query, filter);
    }

    @Override
    public Set<UUID> getRootEntities() {
        return Set.of(filter.getRootEntity().getId());
    }

    @Override
    public EntitySearchDirection getDirection() {
        return filter.getDirection();
    }

    @Override
    public int getMaxLevel() {
        return filter.getMaxLevel();
    }

    @Override
    public boolean isFetchLastLevelOnly() {
        return filter.isFetchLastLevelOnly();
    }

    public abstract EntityType getEntityType();

    @Override
    protected boolean check(RelationInfo relationInfo) {
        EntityData<?> target = relationInfo.getTarget();
        return (filter.getRelationType() == null || relationInfo.getType().equals(filter.getRelationType())) &&
                getEntityType().equals(target.getEntityType()) && super.matches(target);
    }

}
