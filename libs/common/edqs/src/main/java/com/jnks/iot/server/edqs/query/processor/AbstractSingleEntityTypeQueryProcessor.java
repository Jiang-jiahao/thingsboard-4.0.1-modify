package com.jnks.iot.server.edqs.query.processor;

import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.permission.QueryContext;
import com.jnks.iot.server.common.data.query.EntityFilter;
import com.jnks.iot.server.edqs.data.EntityData;
import com.jnks.iot.server.edqs.query.EdqsQuery;
import com.jnks.iot.server.edqs.query.SortableEntityData;
import com.jnks.iot.server.edqs.repo.TenantRepo;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public abstract class AbstractSingleEntityTypeQueryProcessor<T extends EntityFilter> extends AbstractQueryProcessor<T> {

    public AbstractSingleEntityTypeQueryProcessor(TenantRepo repository, QueryContext ctx, EdqsQuery query, T filter) {
        super(repository, ctx, query, filter);
    }

    @Override
    public List<SortableEntityData> processQuery() {
        if (ctx.isTenantUser()) {
            return processTenantQuery();
        } else {
            return processCustomerQuery(ctx.getCustomerId().getId());
        }
    }

    @Override
    public long count() {
        AtomicLong result = new AtomicLong();
        Consumer<EntityData<?>> counter = ed -> result.incrementAndGet();

        if (ctx.isIgnorePermissionCheck()) {
            processAll(counter);
        } else if (ctx.isTenantUser()) {
            processAll(counter);
        } else {
            processCustomerQuery(ctx.getCustomerId().getId(), counter);
        }
        return result.get();
    }

    protected List<SortableEntityData> processTenantQuery() {
        List<SortableEntityData> result = new ArrayList<>(getProbableResultSize());
        processAll(ed -> {
            result.add(toSortData(ed));
        });
        return result;
    }

    protected List<SortableEntityData> processCustomerQuery(UUID customerId) {
        List<SortableEntityData> result = new ArrayList<>(getProbableResultSize());
        processCustomerQuery(customerId, ed -> {
            result.add(toSortData(ed));
        });
        return result;
    }

    protected abstract void processCustomerQuery(UUID customerId, Consumer<EntityData<?>> processor);

    protected abstract void processAll(Consumer<EntityData<?>> processor);

    protected abstract int getProbableResultSize();

}
