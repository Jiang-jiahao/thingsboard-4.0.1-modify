package com.jnks.iot.rule.engine.util;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.rule.engine.api.TbContext;
import com.jnks.iot.rule.engine.api.TbNodeException;
import com.jnks.iot.server.common.data.HasCustomerId;
import com.jnks.iot.server.common.data.id.AssetId;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.UserId;

public class EntitiesCustomerIdAsyncLoader {

    public static ListenableFuture<CustomerId> findEntityIdAsync(TbContext ctx, EntityId originator) {
        switch (originator.getEntityType()) {
            case CUSTOMER:
                return Futures.immediateFuture((CustomerId) originator);
            case USER:
                return toCustomerIdAsync(ctx, ctx.getUserService().findUserByIdAsync(ctx.getTenantId(), (UserId) originator));
            case ASSET:
                return toCustomerIdAsync(ctx, ctx.getAssetService().findAssetByIdAsync(ctx.getTenantId(), (AssetId) originator));
            case DEVICE:
                return toCustomerIdAsync(ctx, Futures.immediateFuture(ctx.getDeviceService().findDeviceById(ctx.getTenantId(), (DeviceId) originator)));
            default:
                return Futures.immediateFailedFuture(new TbNodeException("Unexpected originator EntityType: " + originator.getEntityType()));
        }
    }

    private static <T extends HasCustomerId> ListenableFuture<CustomerId> toCustomerIdAsync(TbContext ctx, ListenableFuture<T> future) {
        return Futures.transform(future, in -> in != null ? in.getCustomerId() : null, ctx.getDbCallbackExecutor());
    }

}
