package com.jnks.iot.server.edqs.query.processor;

import com.jnks.iot.server.common.data.permission.QueryContext;
import com.jnks.iot.server.edqs.query.EdqsQuery;
import com.jnks.iot.server.edqs.repo.TenantRepo;

public class EntityQueryProcessorFactory {

    public static EntityQueryProcessor create(TenantRepo repo, QueryContext ctx, EdqsQuery query) {
        return switch (query.getEntityFilter().getType()) {
            case SINGLE_ENTITY -> new SingleEntityQueryProcessor(repo, ctx, query);
            case ENTITY_LIST -> new EntityListQueryProcessor(repo, ctx, query);
            case ENTITY_NAME -> new EntityNameQueryProcessor(repo, ctx, query);
            case ENTITY_TYPE -> new EntityTypeQueryProcessor(repo, ctx, query);
            case DEVICE_TYPE -> new DeviceTypeQueryProcessor(repo, ctx, query);
            case ASSET_TYPE -> new AssetTypeQueryProcessor(repo, ctx, query);
            case ENTITY_VIEW_TYPE -> new EntityViewTypeQueryProcessor(repo, ctx, query);
            case RELATIONS_QUERY -> new RelationQueryProcessor(repo, ctx, query);
            case API_USAGE_STATE -> new ApiUsageStateQueryProcessor(repo, ctx, query);
            case ASSET_SEARCH_QUERY -> new AssetSearchQueryProcessor(repo, ctx, query);
            case DEVICE_SEARCH_QUERY -> new DeviceSearchQueryProcessor(repo, ctx, query);
            case ENTITY_VIEW_SEARCH_QUERY -> new EntityViewSearchQueryProcessor(repo, ctx, query);
            default -> throw new RuntimeException("Not Implemented!");
        };
    }

}
