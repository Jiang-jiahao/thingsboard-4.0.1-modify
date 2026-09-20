package com.jnks.iot.server.edqs.query.processor;

import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.permission.QueryContext;
import com.jnks.iot.server.common.data.query.EntityViewSearchQueryFilter;
import com.jnks.iot.server.edqs.data.EntityData;
import com.jnks.iot.server.edqs.data.RelationInfo;
import com.jnks.iot.server.edqs.query.EdqsQuery;
import com.jnks.iot.server.edqs.repo.TenantRepo;

public class EntityViewSearchQueryProcessor extends AbstractEntitySearchQueryProcessor<EntityViewSearchQueryFilter> {

    public EntityViewSearchQueryProcessor(TenantRepo repo, QueryContext ctx, EdqsQuery query) {
        super(repo, ctx, query, (EntityViewSearchQueryFilter) query.getEntityFilter());
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.ENTITY_VIEW;
    }

    @Override
    protected boolean check(RelationInfo relationInfo) {
        EntityData<?> ed = relationInfo.getTarget();
        return super.check(relationInfo) &&
                (filter.getEntityViewTypes() == null || filter.getEntityViewTypes().contains(ed.getFields().getType()));
    }

}
