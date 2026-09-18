package org.thingsboard.server.service.cf;

import com.google.common.util.concurrent.ListenableFuture;
import org.thingsboard.server.actors.calculatedField.CalculatedFieldTelemetryMsg;
import org.thingsboard.server.common.data.cf.configuration.Argument;
import org.thingsboard.server.common.data.id.CalculatedFieldId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.msg.queue.TbCallback;
import org.thingsboard.server.service.cf.ctx.CalculatedFieldEntityCtxId;
import org.thingsboard.server.service.cf.ctx.state.ArgumentEntry;
import org.thingsboard.server.service.cf.ctx.state.CalculatedFieldCtx;
import org.thingsboard.server.service.cf.ctx.state.CalculatedFieldState;

import java.util.List;
import java.util.Map;

/**
 * 提供计算字段的核心业务逻辑
 */
public interface CalculatedFieldProcessingService {

    ListenableFuture<CalculatedFieldState> fetchStateFromDb(CalculatedFieldCtx ctx, EntityId entityId);

    Map<String, ArgumentEntry> fetchArgsFromDb(TenantId tenantId, EntityId entityId, Map<String, Argument> arguments);

    void pushMsgToRuleEngine(TenantId tenantId, EntityId entityId, CalculatedFieldResult calculationResult, List<CalculatedFieldId> cfIds, TbCallback callback);

    void pushMsgToLinks(CalculatedFieldTelemetryMsg msg, List<CalculatedFieldEntityCtxId> linkedCalculatedFields, TbCallback callback);

}
