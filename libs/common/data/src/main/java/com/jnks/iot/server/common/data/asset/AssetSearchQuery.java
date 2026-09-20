package com.jnks.iot.server.common.data.asset;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.relation.EntityRelation;
import com.jnks.iot.server.common.data.relation.EntityRelationsQuery;
import com.jnks.iot.server.common.data.relation.RelationEntityTypeFilter;
import com.jnks.iot.server.common.data.relation.RelationsSearchParameters;

import java.util.Collections;
import java.util.List;

/**
 * Created by ashvayka on 03.05.17.
 */
@Data
public class AssetSearchQuery {

    @Schema(description = "Main search parameters.")
    private RelationsSearchParameters parameters;
    @Schema(description = "Type of the relation between root entity and asset (e.g. 'Contains' or 'Manages').")
    private String relationType;
    @Schema(description = "Array of asset types to filter the related entities (e.g. 'Building', 'Vehicle').")
    private List<String> assetTypes;

    public EntityRelationsQuery toEntitySearchQuery() {
        EntityRelationsQuery query = new EntityRelationsQuery();
        query.setParameters(parameters);
        query.setFilters(
                Collections.singletonList(new RelationEntityTypeFilter(relationType == null ? EntityRelation.CONTAINS_TYPE : relationType,
                        Collections.singletonList(EntityType.ASSET))));
        return query;
    }
}
