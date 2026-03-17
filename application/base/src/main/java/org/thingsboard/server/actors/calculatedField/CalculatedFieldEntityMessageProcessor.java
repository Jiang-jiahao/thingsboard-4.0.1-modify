/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.actors.calculatedField;

import com.google.common.util.concurrent.ListenableFuture;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.common.util.DebugModeUtil;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.server.actors.ActorSystemContext;
import org.thingsboard.server.actors.TbActorCtx;
import org.thingsboard.server.actors.shared.AbstractContextAwareMsgProcessor;
import org.thingsboard.server.common.data.AttributeScope;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.cf.configuration.Argument;
import org.thingsboard.server.common.data.cf.configuration.ArgumentType;
import org.thingsboard.server.common.data.cf.configuration.ReferencedEntityKey;
import org.thingsboard.server.common.data.id.CalculatedFieldId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.StringDataEntry;
import org.thingsboard.server.common.data.msg.TbMsgType;
import org.thingsboard.server.common.msg.cf.CalculatedFieldPartitionChangeMsg;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.common.msg.queue.TbCallback;
import org.thingsboard.server.gen.transport.TransportProtos.AttributeScopeProto;
import org.thingsboard.server.gen.transport.TransportProtos.AttributeValueProto;
import org.thingsboard.server.gen.transport.TransportProtos.CalculatedFieldTelemetryMsgProto;
import org.thingsboard.server.gen.transport.TransportProtos.TsKvProto;
import org.thingsboard.server.service.cf.CalculatedFieldProcessingService;
import org.thingsboard.server.service.cf.CalculatedFieldResult;
import org.thingsboard.server.service.cf.CalculatedFieldStateService;
import org.thingsboard.server.service.cf.ctx.CalculatedFieldEntityCtxId;
import org.thingsboard.server.service.cf.ctx.state.ArgumentEntry;
import org.thingsboard.server.service.cf.ctx.state.CalculatedFieldCtx;
import org.thingsboard.server.service.cf.ctx.state.CalculatedFieldState;
import org.thingsboard.server.service.cf.ctx.state.SingleValueArgumentEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * 负责处理特定实体（Entity）的计算字段（Calculated Field）相关消息的Actor消息处理器。
 * 每个实体对应一个此处理器实例，管理该实体下所有计算字段的状态（states），并响应各类事件（如遥测数据更新、属性更新、删除等）。
 * @author Andrew Shvayka
 */
@Slf4j
public class CalculatedFieldEntityMessageProcessor extends AbstractContextAwareMsgProcessor {
    // (1 for result persistence + 1 for the state persistence )
    // 每个计算字段处理完成后需要两个回调：一个用于持久化结果，一个用于持久化状态
    public static final int CALLBACKS_PER_CF = 2;

    final TenantId tenantId;
    final EntityId entityId;

    // 计算字段处理服务（负责计算逻辑、推送结果等）
    final CalculatedFieldProcessingService cfService;

    // 计算字段状态持久化状态服务
    final CalculatedFieldStateService cfStateService;

    // Actor上下文，用于与Actor系统交互
    TbActorCtx ctx;

    // 内存中维护的该实体下所有计算字段的最新状态，键为计算字段ID
    Map<CalculatedFieldId, CalculatedFieldState> states = new HashMap<>();

    CalculatedFieldEntityMessageProcessor(ActorSystemContext systemContext, TenantId tenantId, EntityId entityId) {
        super(systemContext);
        this.tenantId = tenantId;
        this.entityId = entityId;
        this.cfService = systemContext.getCalculatedFieldProcessingService();
        this.cfStateService = systemContext.getCalculatedFieldStateService();
    }

    // 初始化时设置Actor上下文
    void init(TbActorCtx ctx) {
        this.ctx = ctx;
    }

    // 停止处理器，清理状态并停止自身Actor
    public void stop() {
        log.info("[{}][{}] Stopping entity actor.", tenantId, entityId);
        states.clear();
        ctx.stop(ctx.getSelf());
    }

    /**
     * 处理分区变更消息。如果当前节点不再是该实体对应分区的主人，则停止自身Actor。
     */
    public void process(CalculatedFieldPartitionChangeMsg msg) {
        if (!systemContext.getPartitionService().resolve(ServiceType.TB_RULE_ENGINE, DataConstants.CF_QUEUE_NAME, tenantId, entityId).isMyPartition()) {
            log.info("[{}] Stopping entity actor due to change partition event.", entityId);
            ctx.stop(ctx.getSelf());
        }
    }

    /**
     * 处理状态恢复消息。从外部恢复某个计算字段的状态（例如从数据库加载后），更新本地内存。
     */
    public void process(CalculatedFieldStateRestoreMsg msg) {
        CalculatedFieldId cfId = msg.getId().cfId();
        log.debug("[{}] [{}] Processing CF state restore msg.", msg.getId().entityId(), cfId);
        if (msg.getState() != null) {
            states.put(cfId, msg.getState());
        } else {
            states.remove(cfId);
        }
    }

    /**
     * 处理实体初始化计算字段消息。首次启动或强制重新初始化时，从数据库加载状态，并尝试执行计算。
     */
    public void process(EntityInitCalculatedFieldMsg msg) throws CalculatedFieldException {
        log.debug("[{}] Processing entity init CF msg.", msg.getCtx().getCfId());
        var ctx = msg.getCtx();
        if (msg.isForceReinit()) {
            log.debug("Force reinitialization of CF: [{}].", ctx.getCfId());
            states.remove(ctx.getCfId());
        }
        try {
            // 获取或初始化状态
            var state = getOrInitState(ctx);
            if (state.isSizeOk()) {
                // 如果状态就绪，执行计算并持久化
                processStateIfReady(ctx, Collections.singletonList(ctx.getCfId()), state, null, null, msg.getCallback());
            } else {
                throw new RuntimeException(ctx.getSizeExceedsLimitMessage());
            }
        } catch (Exception e) {
            if (e instanceof CalculatedFieldException cfe) {
                throw cfe;
            }
            throw CalculatedFieldException.builder().ctx(ctx).eventEntity(entityId).cause(e).build();
        }
    }

    /**
     * 处理删除计算字段或整个实体的消息。
     * 如果是删除整个实体，则移除所有计算字段的状态并停止Actor；
     * 如果是删除单个计算字段，则只移除该字段的状态。
     */
    public void process(CalculatedFieldEntityDeleteMsg msg) {
        log.debug("[{}] Processing CF entity delete msg.", msg.getEntityId());
        if (this.entityId.equals(msg.getEntityId())) {
            // 删除整个实体
            if (states.isEmpty()) {
                msg.getCallback().onSuccess();
            } else {
                MultipleTbCallback multipleTbCallback = new MultipleTbCallback(states.size(), msg.getCallback());
                states.forEach((cfId, state) -> cfStateService.removeState(new CalculatedFieldEntityCtxId(tenantId, cfId, entityId), multipleTbCallback));
                ctx.stop(ctx.getSelf());
            }
        } else {
            // 删除单个计算字段（此时msg.getEntityId()实际上是计算字段ID）
            var cfId = new CalculatedFieldId(msg.getEntityId().getId());
            var state = states.remove(cfId);
            if (state != null) {
                cfStateService.removeState(new CalculatedFieldEntityCtxId(tenantId, cfId, entityId), msg.getCallback());
            } else {
                msg.getCallback().onSuccess();
            }
        }
    }

    /**
     * 处理实体的遥测数据消息（包括多个计算字段的上下文）。
     * 根据消息中的字段列表，为每个相关的计算字段处理遥测更新。
     */
    public void process(EntityCalculatedFieldTelemetryMsg msg) throws CalculatedFieldException {
        log.debug("[{}] Processing CF telemetry msg.", msg.getEntityId());
        var proto = msg.getProto();
        // 计算需要的回调总数：每个计算字段需要CALLBACKS_PER_CF个回调，乘以涉及的字段数
        var numberOfCallbacks = CALLBACKS_PER_CF * (msg.getEntityIdFields().size() + msg.getProfileIdFields().size());
        MultipleTbCallback callback = new MultipleTbCallback(numberOfCallbacks, msg.getCallback());
        // 获取消息中已处理过的计算字段ID（用于避免重复触发）
        List<CalculatedFieldId> cfIdList = getCalculatedFieldIds(proto);
        Set<CalculatedFieldId> cfIdSet = new HashSet<>(cfIdList);
        // 分别处理实体字段和配置字段
        for (var ctx : msg.getEntityIdFields()) {
            process(ctx, proto, cfIdSet, cfIdList, callback);
        }
        for (var ctx : msg.getProfileIdFields()) {
            process(ctx, proto, cfIdSet, cfIdList, callback);
        }
    }

    /**
     * 处理单个计算字段的关联遥测数据消息（例如来自链路的更新）。
     * 根据消息类型（遥测、属性、删除等）更新对应参数值，并触发计算。
     */
    public void process(EntityCalculatedFieldLinkedTelemetryMsg msg) throws CalculatedFieldException {
        log.debug("[{}] Processing CF link telemetry msg.", msg.getEntityId());
        var proto = msg.getProto();
        var ctx = msg.getCtx();
        var callback = new MultipleTbCallback(CALLBACKS_PER_CF, msg.getCallback());
        try {
            List<CalculatedFieldId> cfIds = getCalculatedFieldIds(proto);
            if (cfIds.contains(ctx.getCfId())) {
                // 如果该计算字段已经在消息的“已处理”列表中，则直接成功（避免循环触发）
                callback.onSuccess(CALLBACKS_PER_CF);
            } else {
                // 根据消息类型（遥测、属性、删除）将数据转换为参数条目，并更新状态
                if (proto.getTsDataCount() > 0) {
                    processArgumentValuesUpdate(ctx, cfIds, callback, mapToArguments(ctx, msg.getEntityId(), proto.getTsDataList()), toTbMsgId(proto), toTbMsgType(proto));
                } else if (proto.getAttrDataCount() > 0) {
                    processArgumentValuesUpdate(ctx, cfIds, callback, mapToArguments(ctx, msg.getEntityId(), proto.getScope(), proto.getAttrDataList()), toTbMsgId(proto), toTbMsgType(proto));
                } else if (proto.getRemovedTsKeysCount() > 0) {
                    processArgumentValuesUpdate(ctx, cfIds, callback, mapToArgumentsWithFetchedValue(ctx, proto.getRemovedTsKeysList()), toTbMsgId(proto), toTbMsgType(proto));
                } else if (proto.getRemovedAttrKeysCount() > 0) {
                    processArgumentValuesUpdate(ctx, cfIds, callback, mapToArgumentsWithDefaultValue(ctx, msg.getEntityId(), proto.getScope(), proto.getRemovedAttrKeysList()), toTbMsgId(proto), toTbMsgType(proto));
                } else {
                    callback.onSuccess(CALLBACKS_PER_CF);
                }
            }
        } catch (Exception e) {
            throw CalculatedFieldException.builder().ctx(ctx).eventEntity(entityId).cause(e).build();
        }
    }

    /**
     * 处理单个计算字段上下文中的遥测消息，根据消息类型调用对应的更新方法。
     */
    private void process(CalculatedFieldCtx ctx, CalculatedFieldTelemetryMsgProto proto, Collection<CalculatedFieldId> cfIds, List<CalculatedFieldId> cfIdList, MultipleTbCallback callback) throws CalculatedFieldException {
        try {
            if (cfIds.contains(ctx.getCfId())) {
                callback.onSuccess(CALLBACKS_PER_CF);
            } else {
                if (proto.getTsDataCount() > 0) {
                    processTelemetry(ctx, proto, cfIdList, callback);
                } else if (proto.getAttrDataCount() > 0) {
                    processAttributes(ctx, proto, cfIdList, callback);
                } else if (proto.getRemovedTsKeysCount() > 0) {
                    processRemovedTelemetry(ctx, proto, cfIdList, callback);
                } else if (proto.getRemovedAttrKeysCount() > 0) {
                    processRemovedAttributes(ctx, proto, cfIdList, callback);
                } else {
                    callback.onSuccess(CALLBACKS_PER_CF);
                }
            }
        } catch (Exception e) {
            if (e instanceof CalculatedFieldException cfe) {
                throw cfe;
            }
            throw CalculatedFieldException.builder().ctx(ctx).eventEntity(entityId).cause(e).build();
        }
    }

    private void processTelemetry(CalculatedFieldCtx ctx, CalculatedFieldTelemetryMsgProto proto, List<CalculatedFieldId> cfIdList, MultipleTbCallback callback) throws CalculatedFieldException {
        processArgumentValuesUpdate(ctx, cfIdList, callback, mapToArguments(ctx, proto.getTsDataList()), toTbMsgId(proto), toTbMsgType(proto));
    }

    private void processAttributes(CalculatedFieldCtx ctx, CalculatedFieldTelemetryMsgProto proto, List<CalculatedFieldId> cfIdList, MultipleTbCallback callback) throws CalculatedFieldException {
        processArgumentValuesUpdate(ctx, cfIdList, callback, mapToArguments(ctx, proto.getScope(), proto.getAttrDataList()), toTbMsgId(proto), toTbMsgType(proto));
    }

    private void processRemovedTelemetry(CalculatedFieldCtx ctx, CalculatedFieldTelemetryMsgProto proto, List<CalculatedFieldId> cfIdList, MultipleTbCallback callback) throws CalculatedFieldException {
        processArgumentValuesUpdate(ctx, cfIdList, callback, mapToArgumentsWithFetchedValue(ctx, proto.getRemovedTsKeysList()), toTbMsgId(proto), toTbMsgType(proto));
    }

    private void processRemovedAttributes(CalculatedFieldCtx ctx, CalculatedFieldTelemetryMsgProto proto, List<CalculatedFieldId> cfIdList, MultipleTbCallback callback) throws CalculatedFieldException {
        processArgumentValuesUpdate(ctx, cfIdList, callback, mapToArgumentsWithDefaultValue(ctx, proto.getScope(), proto.getRemovedAttrKeysList()), toTbMsgId(proto), toTbMsgType(proto));
    }

    /**
     * 核心方法：用新的参数值更新指定计算字段的状态，如果状态就绪则执行计算并持久化结果和状态。
     *
     * @param ctx         计算字段上下文
     * @param cfIdList    本次消息中已经处理过的计算字段ID列表（用于避免重复触发）
     * @param callback    回调
     * @param newArgValues 新的参数值映射（参数名 -> 参数条目）
     * @param tbMsgId     触发消息的ID（用于调试）
     * @param tbMsgType   触发消息的类型（用于调试）
     */
    private void processArgumentValuesUpdate(CalculatedFieldCtx ctx, List<CalculatedFieldId> cfIdList, MultipleTbCallback callback,
                                             Map<String, ArgumentEntry> newArgValues, UUID tbMsgId, TbMsgType tbMsgType) throws CalculatedFieldException {
        if (newArgValues.isEmpty()) {
            log.debug("[{}] No new argument values to process for CF.", ctx.getCfId());
            callback.onSuccess(CALLBACKS_PER_CF);
        }
        CalculatedFieldState state = states.get(ctx.getCfId());
        boolean justRestored = false;
        if (state == null) {
            state = getOrInitState(ctx);
            justRestored = true;
        }
        if (state.isSizeOk()) {
            // 更新状态，如果状态发生了变化（或刚刚恢复），则准备执行计算
            if (state.updateState(ctx, newArgValues) || justRestored) {
                // 将当前计算字段ID加入列表（表示将要触发计算）
                cfIdList = new ArrayList<>(cfIdList);
                cfIdList.add(ctx.getCfId());
                processStateIfReady(ctx, cfIdList, state, tbMsgId, tbMsgType, callback);
            } else {
                callback.onSuccess(CALLBACKS_PER_CF);
            }
        } else {
            throw CalculatedFieldException.builder().ctx(ctx).eventEntity(entityId).errorMessage(ctx.getSizeExceedsLimitMessage()).build();
        }
    }

    /**
     * 获取或初始化指定计算字段的状态。
     * 先从本地内存获取，若不存在则从数据库异步加载（同步等待最多1分钟）。
     */
    @SneakyThrows
    private CalculatedFieldState getOrInitState(CalculatedFieldCtx ctx) {
        CalculatedFieldState state = states.get(ctx.getCfId());
        if (state != null) {
            return state;
        } else {
            ListenableFuture<CalculatedFieldState> stateFuture = systemContext.getCalculatedFieldProcessingService().fetchStateFromDb(ctx, entityId);
            // Ugly but necessary. We do not expect to often fetch data from DB. Only once per <Entity, CalculatedField> pair lifetime.
            // This call happens while processing the CF pack from the queue consumer. So the timeout should be relatively low.
            // Alternatively, we can fetch the state outside the actor system and push separate command to create this actor,
            // but this will significantly complicate the code.
            // 阻塞等待状态加载完成，因为此操作发生频率很低（每个<实体，计算字段>对只发生一次）
            state = stateFuture.get(1, TimeUnit.MINUTES);
            state.checkStateSize(new CalculatedFieldEntityCtxId(tenantId, ctx.getCfId(), entityId), ctx.getMaxStateSize());
            states.put(ctx.getCfId(), state);
        }
        return state;
    }

    /**
     * 如果状态已就绪，执行计算并推送结果到规则引擎，同时持久化状态。
     *
     * @param ctx         计算字段上下文
     * @param cfIdList    本次消息中所有需要标记为“已处理”的计算字段ID（用于避免循环）
     * @param state       当前状态
     * @param tbMsgId     触发消息ID（调试）
     * @param tbMsgType   触发消息类型（调试）
     * @param callback    回调
     */
    private void processStateIfReady(CalculatedFieldCtx ctx, List<CalculatedFieldId> cfIdList, CalculatedFieldState state, UUID tbMsgId, TbMsgType tbMsgType, TbCallback callback) throws CalculatedFieldException {
        CalculatedFieldEntityCtxId ctxId = new CalculatedFieldEntityCtxId(tenantId, ctx.getCfId(), entityId);
        boolean stateSizeChecked = false;
        try {
            if (ctx.isInitialized() && state.isReady()) {
                // 执行计算（异步，等待计算结果超时时间）
                CalculatedFieldResult calculationResult = state.performCalculation(ctx).get(systemContext.getCfCalculationResultTimeout(), TimeUnit.SECONDS);
                state.checkStateSize(ctxId, ctx.getMaxStateSize());
                stateSizeChecked = true;
                if (state.isSizeOk()) {
                    if (!calculationResult.isEmpty()) {
                        // 推送计算结果到规则引擎，并传入cfIdList用于避免循环
                        cfService.pushMsgToRuleEngine(tenantId, entityId, calculationResult, cfIdList, callback);
                    } else {
                        callback.onSuccess();
                    }
                    // 调试模式：记录调试事件
                    if (DebugModeUtil.isDebugAllAvailable(ctx.getCalculatedField())) {
                        systemContext.persistCalculatedFieldDebugEvent(tenantId, ctx.getCfId(), entityId, state.getArguments(), tbMsgId, tbMsgType, JacksonUtil.writeValueAsString(calculationResult.getResult()), null);
                    }
                }
            } else {
                callback.onSuccess();
            }
        } catch (Exception e) {
            throw CalculatedFieldException.builder().ctx(ctx).eventEntity(entityId).msgId(tbMsgId).msgType(tbMsgType).arguments(state.getArguments()).cause(e).build();
        } finally {
            if (!stateSizeChecked) {
                state.checkStateSize(ctxId, ctx.getMaxStateSize());
            }
            if (state.isSizeOk()) {
                // 持久化状态
                cfStateService.persistState(ctxId, state, callback);
            } else {
                // 状态大小超限，移除状态并抛出异常
                removeStateAndRaiseSizeException(ctxId, CalculatedFieldException.builder().ctx(ctx).eventEntity(entityId).errorMessage(ctx.getSizeExceedsLimitMessage()).build(), callback);
            }
        }
    }

    /**
     * 移除超限状态并抛出异常。
     */
    private void removeStateAndRaiseSizeException(CalculatedFieldEntityCtxId ctxId, CalculatedFieldException ex, TbCallback callback) throws CalculatedFieldException {
        // We remove the state, but remember that it is over-sized in a local map.
        cfStateService.removeState(ctxId, new TbCallback() {
            @Override
            public void onSuccess() {
                callback.onFailure(ex);
            }

            @Override
            public void onFailure(Throwable t) {
                callback.onFailure(ex);
            }
        });
        throw ex;
    }

    // ---- 参数映射方法（将Protobuf消息转换为ArgumentEntry） ----
    private Map<String, ArgumentEntry> mapToArguments(CalculatedFieldCtx ctx, List<TsKvProto> data) {
        return mapToArguments(ctx.getMainEntityArguments(), data);
    }

    private Map<String, ArgumentEntry> mapToArguments(CalculatedFieldCtx ctx, EntityId entityId, List<TsKvProto> data) {
        var argNames = ctx.getLinkedEntityArguments().get(entityId);
        if (argNames.isEmpty()) {
            return Collections.emptyMap();
        }
        return mapToArguments(argNames, data);
    }

    private Map<String, ArgumentEntry> mapToArguments(Map<ReferencedEntityKey, String> argNames, List<TsKvProto> data) {
        if (argNames.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, ArgumentEntry> arguments = new HashMap<>();
        for (TsKvProto item : data) {
            ReferencedEntityKey key = new ReferencedEntityKey(item.getKv().getKey(), ArgumentType.TS_LATEST, null);
            String argName = argNames.get(key);
            if (argName != null) {
                arguments.put(argName, new SingleValueArgumentEntry(item));
            }
            key = new ReferencedEntityKey(item.getKv().getKey(), ArgumentType.TS_ROLLING, null);
            argName = argNames.get(key);
            if (argName != null) {
                arguments.put(argName, new SingleValueArgumentEntry(item));
            }
        }
        return arguments;
    }

    private Map<String, ArgumentEntry> mapToArguments(CalculatedFieldCtx ctx, AttributeScopeProto scope, List<AttributeValueProto> attrDataList) {
        return mapToArguments(ctx.getMainEntityArguments(), scope, attrDataList);
    }

    private Map<String, ArgumentEntry> mapToArguments(CalculatedFieldCtx ctx, EntityId entityId, AttributeScopeProto scope, List<AttributeValueProto> attrDataList) {
        var argNames = ctx.getLinkedEntityArguments().get(entityId);
        if (argNames.isEmpty()) {
            return Collections.emptyMap();
        }
        return mapToArguments(argNames, scope, attrDataList);
    }

    private Map<String, ArgumentEntry> mapToArguments(Map<ReferencedEntityKey, String> argNames, AttributeScopeProto scope, List<AttributeValueProto> attrDataList) {
        Map<String, ArgumentEntry> arguments = new HashMap<>();
        for (AttributeValueProto item : attrDataList) {
            ReferencedEntityKey key = new ReferencedEntityKey(item.getKey(), ArgumentType.ATTRIBUTE, AttributeScope.valueOf(scope.name()));
            String argName = argNames.get(key);
            if (argName != null) {
                arguments.put(argName, new SingleValueArgumentEntry(item));
            }
        }
        return arguments;
    }

    private Map<String, ArgumentEntry> mapToArgumentsWithDefaultValue(CalculatedFieldCtx ctx, EntityId entityId, AttributeScopeProto scope, List<String> removedAttrKeys) {
        var argNames = ctx.getLinkedEntityArguments().get(entityId);
        if (argNames.isEmpty()) {
            return Collections.emptyMap();
        }
        return mapToArgumentsWithDefaultValue(argNames, ctx.getArguments(), scope, removedAttrKeys);
    }

    private Map<String, ArgumentEntry> mapToArgumentsWithDefaultValue(CalculatedFieldCtx ctx, AttributeScopeProto scope, List<String> removedAttrKeys) {
        return mapToArgumentsWithDefaultValue(ctx.getMainEntityArguments(), ctx.getArguments(), scope, removedAttrKeys);
    }

    private Map<String, ArgumentEntry> mapToArgumentsWithDefaultValue(Map<ReferencedEntityKey, String> argNames, Map<String, Argument> configArguments, AttributeScopeProto scope, List<String> removedAttrKeys) {
        Map<String, ArgumentEntry> arguments = new HashMap<>();
        for (String removedKey : removedAttrKeys) {
            ReferencedEntityKey key = new ReferencedEntityKey(removedKey, ArgumentType.ATTRIBUTE, AttributeScope.valueOf(scope.name()));
            String argName = argNames.get(key);
            if (argName != null) {
                Argument argument = configArguments.get(argName);
                String defaultValue = (argument != null) ? argument.getDefaultValue() : null;
                arguments.put(argName, StringUtils.isNotEmpty(defaultValue)
                        ? new SingleValueArgumentEntry(System.currentTimeMillis(), new StringDataEntry(removedKey, defaultValue), null)
                        : new SingleValueArgumentEntry());

            }
        }
        return arguments;
    }

    /**
     * 处理遥测键删除时，需要从数据库中获取最新的值作为参数（因为删除意味着清空，但计算可能需要最后一次值）。
     */
    private Map<String, ArgumentEntry> mapToArgumentsWithFetchedValue(CalculatedFieldCtx ctx, List<String> removedTelemetryKeys) {
        Map<String, Argument> deletedArguments = ctx.getArguments().entrySet().stream()
                .filter(entry -> removedTelemetryKeys.contains(entry.getValue().getRefEntityKey().getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<String, ArgumentEntry> fetchedArgs = cfService.fetchArgsFromDb(tenantId, entityId, deletedArguments);
        // 强制重置之前的值（因为是删除事件）
        fetchedArgs.values().forEach(arg -> arg.setForceResetPrevious(true));
        return fetchedArgs;
    }

    // 从Protobuf消息中提取已处理的计算字段ID列表（用于避免循环触发）
    private static List<CalculatedFieldId> getCalculatedFieldIds(CalculatedFieldTelemetryMsgProto proto) {
        List<CalculatedFieldId> cfIds = new LinkedList<>();
        for (var cfId : proto.getPreviousCalculatedFieldsList()) {
            cfIds.add(new CalculatedFieldId(new UUID(cfId.getCalculatedFieldIdMSB(), cfId.getCalculatedFieldIdLSB())));
        }
        return cfIds;
    }

    private UUID toTbMsgId(CalculatedFieldTelemetryMsgProto proto) {
        if (proto.getTbMsgIdMSB() != 0 && proto.getTbMsgIdLSB() != 0) {
            return new UUID(proto.getTbMsgIdMSB(), proto.getTbMsgIdLSB());
        }
        return null;
    }

    private TbMsgType toTbMsgType(CalculatedFieldTelemetryMsgProto proto) {
        if (!proto.getTbMsgType().isEmpty()) {
            return TbMsgType.valueOf(proto.getTbMsgType());
        }
        return null;
    }

}
