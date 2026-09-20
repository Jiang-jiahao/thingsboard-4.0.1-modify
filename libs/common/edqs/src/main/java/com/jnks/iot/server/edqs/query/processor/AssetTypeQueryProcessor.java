package com.jnks.iot.server.edqs.query.processor;

import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.permission.QueryContext;
import com.jnks.iot.server.common.data.query.AssetTypeFilter;
import com.jnks.iot.server.edqs.query.EdqsQuery;
import com.jnks.iot.server.edqs.repo.TenantRepo;

import java.util.List;

public class AssetTypeQueryProcessor extends AbstractEntityProfileQueryProcessor<AssetTypeFilter> {

    public AssetTypeQueryProcessor(TenantRepo repo, QueryContext ctx, EdqsQuery query) {
        super(repo, ctx, query, (AssetTypeFilter) query.getEntityFilter(), EntityType.ASSET);
    }

    @Override
    protected String getEntityNameFilter(AssetTypeFilter filter) {
        return filter.getAssetNameFilter();
    }

    @Override
    protected List<String> getProfileNames(AssetTypeFilter filter) {
        return filter.getAssetTypes();
    }

    @Override
    protected EntityType getProfileEntityType() {
        return EntityType.ASSET_PROFILE;
    }

}
