package com.jnks.iot.server.edqs.query.processor;

import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.permission.QueryContext;
import com.jnks.iot.server.common.data.query.AssetSearchQueryFilter;
import com.jnks.iot.server.common.data.util.CollectionsUtil;
import com.jnks.iot.server.edqs.data.EntityData;
import com.jnks.iot.server.edqs.data.ProfileAwareData;
import com.jnks.iot.server.edqs.data.RelationInfo;
import com.jnks.iot.server.edqs.query.EdqsQuery;
import com.jnks.iot.server.edqs.repo.TenantRepo;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class AssetSearchQueryProcessor extends AbstractEntitySearchQueryProcessor<AssetSearchQueryFilter> {

    private final Set<UUID> entityProfileIds = new HashSet<>();

    public AssetSearchQueryProcessor(TenantRepo repo, QueryContext ctx, EdqsQuery query) {
        super(repo, ctx, query, (AssetSearchQueryFilter) query.getEntityFilter());
        if (CollectionsUtil.isNotEmpty(filter.getAssetTypes())) {
            var profileNamesSet = new HashSet<>(this.filter.getAssetTypes());
            for (EntityData<?> dp : repo.getEntitySet(EntityType.ASSET_PROFILE)) {
                if (profileNamesSet.contains(dp.getFields().getName())) {
                    entityProfileIds.add(dp.getId());
                }
            }
        }
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.ASSET;
    }

    @Override
    protected boolean check(RelationInfo relationInfo) {
        return super.check(relationInfo) &&
                (entityProfileIds.isEmpty() || entityProfileIds.contains(((ProfileAwareData<?>) relationInfo.getTarget()).getFields().getProfileId()));
    }

}
