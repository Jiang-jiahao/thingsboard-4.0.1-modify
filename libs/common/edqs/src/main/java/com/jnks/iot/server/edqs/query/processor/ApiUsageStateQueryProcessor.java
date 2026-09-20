package com.jnks.iot.server.edqs.query.processor;

import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.edqs.fields.ApiUsageStateFields;
import com.jnks.iot.server.common.data.permission.QueryContext;
import com.jnks.iot.server.common.data.query.ApiUsageStateFilter;
import com.jnks.iot.server.edqs.data.CustomerData;
import com.jnks.iot.server.edqs.data.EntityData;
import com.jnks.iot.server.edqs.query.EdqsQuery;
import com.jnks.iot.server.edqs.repo.TenantRepo;

import java.util.UUID;
import java.util.function.Consumer;

public class ApiUsageStateQueryProcessor extends AbstractSingleEntityTypeQueryProcessor<ApiUsageStateFilter> {

    public ApiUsageStateQueryProcessor(TenantRepo repo, QueryContext ctx, EdqsQuery query) {
        super(repo, ctx, query, (ApiUsageStateFilter) query.getEntityFilter());
    }

    @Override
    protected void processCustomerQuery(UUID customerId, Consumer<EntityData<?>> processor) {
        CustomerData customerData = (CustomerData) repository.getEntityMap(EntityType.CUSTOMER).get(customerId);
        if (customerData != null) {
            process(customerData.getEntities(EntityType.API_USAGE_STATE), processor);
        }
    }

    @Override
    protected void processAll(Consumer<EntityData<?>> processor) {
        process(repository.getEntitySet(EntityType.API_USAGE_STATE), processor);
    }

    @Override
    protected boolean matches(EntityData<?> ed) {
        ApiUsageStateFields entityFields = (ApiUsageStateFields) ed.getFields();
        return super.matches(ed) && (filter.getCustomerId() == null || filter.getCustomerId().equals(entityFields.getEntityId()));
    }

    @Override
    protected int getProbableResultSize() {
        return 1;
    }

}
