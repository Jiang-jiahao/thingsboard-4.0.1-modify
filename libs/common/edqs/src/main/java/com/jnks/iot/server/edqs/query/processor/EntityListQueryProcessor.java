package com.jnks.iot.server.edqs.query.processor;

import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.permission.QueryContext;
import com.jnks.iot.server.common.data.query.EntityListFilter;
import com.jnks.iot.server.edqs.data.EntityData;
import com.jnks.iot.server.edqs.query.EdqsQuery;
import com.jnks.iot.server.edqs.repo.TenantRepo;

import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class EntityListQueryProcessor extends AbstractSingleEntityTypeQueryProcessor<EntityListFilter> {

    private final EntityType entityType;
    private final Set<UUID> entityIds;

    public EntityListQueryProcessor(TenantRepo repo, QueryContext ctx, EdqsQuery query) {
        super(repo, ctx, query, (EntityListFilter) query.getEntityFilter());
        this.entityType = filter.getEntityType();
        this.entityIds = filter.getEntityList().stream().map(UUID::fromString).collect(Collectors.toSet());
    }

    @Override
    protected void processCustomerQuery(UUID customerId, Consumer<EntityData<?>> processor) {
        processAll(ed -> {
            if (checkCustomerId(customerId, ed)) {
                processor.accept(ed);
            }
        });
    }

    @Override
    protected void processAll(Consumer<EntityData<?>> processor) {
        var map = repository.getEntityMap(entityType);
        for (UUID entityId : entityIds) {
            EntityData<?> ed = map.get(entityId);
            if (matches(ed)) {
                processor.accept(ed);
            }
        }
    }

    @Override
    protected int getProbableResultSize() {
        return entityIds.size();
    }

}
