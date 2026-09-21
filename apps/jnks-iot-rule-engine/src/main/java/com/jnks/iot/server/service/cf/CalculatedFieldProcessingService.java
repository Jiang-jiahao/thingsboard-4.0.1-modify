package com.jnks.iot.server.service.cf;

import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.server.actors.calculatedField.CalculatedFieldTelemetryMsg;
import com.jnks.iot.server.common.data.cf.configuration.Argument;
import com.jnks.iot.server.common.data.id.CalculatedFieldId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.queue.JnksIotCallback;
import com.jnks.iot.server.service.cf.ctx.CalculatedFieldEntityCtxId;
import com.jnks.iot.server.service.cf.ctx.state.ArgumentEntry;
import com.jnks.iot.server.service.cf.ctx.state.CalculatedFieldCtx;
import com.jnks.iot.server.service.cf.ctx.state.CalculatedFieldState;

import java.util.List;
import java.util.Map;

/**
 * 提供计算字段的核心业务逻辑
 */
public interface CalculatedFieldProcessingService {

    ListenableFuture<CalculatedFieldState> fetchStateFromDb(CalculatedFieldCtx ctx, EntityId entityId);

    Map<String, ArgumentEntry> fetchArgsFromDb(TenantId tenantId, EntityId entityId, Map<String, Argument> arguments);

    void pushMsgToRuleEngine(TenantId tenantId, EntityId entityId, CalculatedFieldResult calculationResult, List<CalculatedFieldId> cfIds, JnksIotCallback callback);

    void pushMsgToLinks(CalculatedFieldTelemetryMsg msg, List<CalculatedFieldEntityCtxId> linkedCalculatedFields, JnksIotCallback callback);

}
