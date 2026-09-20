package com.jnks.iot.rule.engine.util;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.collections4.CollectionUtils;
import com.jnks.iot.rule.engine.api.TbContext;
import com.jnks.iot.rule.engine.data.RelationsQuery;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.relation.EntityRelationsQuery;
import com.jnks.iot.server.common.data.relation.EntitySearchDirection;
import com.jnks.iot.server.common.data.relation.RelationsSearchParameters;

public class EntitiesRelatedEntityIdAsyncLoader {

    public static ListenableFuture<EntityId> findEntityAsync(
            TbContext ctx,
            EntityId originator,
            RelationsQuery relationsQuery
    ) {
        var relationService = ctx.getRelationService();
        var query = buildQuery(originator, relationsQuery);
        var relationListFuture = relationService.findByQuery(ctx.getTenantId(), query);
        if (relationsQuery.getDirection() == EntitySearchDirection.FROM) {
            return Futures.transformAsync(relationListFuture,
                    relationList -> CollectionUtils.isNotEmpty(relationList) ?
                            Futures.immediateFuture(relationList.get(0).getTo())
                            : Futures.immediateFuture(null), ctx.getDbCallbackExecutor());
        } else if (relationsQuery.getDirection() == EntitySearchDirection.TO) {
            return Futures.transformAsync(relationListFuture,
                    relationList -> CollectionUtils.isNotEmpty(relationList) ?
                            Futures.immediateFuture(relationList.get(0).getFrom())
                            : Futures.immediateFuture(null), ctx.getDbCallbackExecutor());
        }
        return Futures.immediateFailedFuture(new IllegalStateException("Unknown direction"));
    }

    private static EntityRelationsQuery buildQuery(EntityId originator, RelationsQuery relationsQuery) {
        var query = new EntityRelationsQuery();
        var parameters = new RelationsSearchParameters(
                originator,
                relationsQuery.getDirection(),
                relationsQuery.getMaxLevel(),
                relationsQuery.isFetchLastLevelOnly()
        );
        query.setParameters(parameters);
        query.setFilters(relationsQuery.getFilters());
        return query;
    }

}
