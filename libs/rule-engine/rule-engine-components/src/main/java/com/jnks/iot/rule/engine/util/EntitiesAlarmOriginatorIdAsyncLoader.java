package com.jnks.iot.rule.engine.util;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.alarm.Alarm;
import com.jnks.iot.server.common.data.id.AlarmId;
import com.jnks.iot.server.common.data.id.EntityId;

public class EntitiesAlarmOriginatorIdAsyncLoader {

    public static ListenableFuture<EntityId> findEntityIdAsync(JnksIotContext ctx, EntityId originator) {
        switch (originator.getEntityType()) {
            case ALARM:
                return getAlarmOriginatorAsync(ctx.getAlarmService().findAlarmByIdAsync(ctx.getTenantId(), (AlarmId) originator), ctx);
            default:
                return Futures.immediateFailedFuture(new JnksIotNodeException("Unexpected originator EntityType " + originator.getEntityType()));
        }
    }

    private static ListenableFuture<EntityId> getAlarmOriginatorAsync(ListenableFuture<Alarm> future, JnksIotContext ctx) {
        return Futures.transformAsync(future, in -> in != null ?
                Futures.immediateFuture(in.getOriginator())
                : Futures.immediateFuture(null), ctx.getDbCallbackExecutor());
    }

}
