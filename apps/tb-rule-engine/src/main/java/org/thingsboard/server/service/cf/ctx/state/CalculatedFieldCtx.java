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
package org.thingsboard.server.service.cf.ctx.state;

import lombok.Data;
import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;
import org.mvel2.MVEL;
import org.thingsboard.script.api.tbel.TbelInvokeService;
import org.thingsboard.server.common.data.AttributeScope;
import org.thingsboard.server.common.data.cf.CalculatedField;
import org.thingsboard.server.common.data.cf.CalculatedFieldType;
import org.thingsboard.server.common.data.cf.configuration.Argument;
import org.thingsboard.server.common.data.cf.configuration.ArgumentType;
import org.thingsboard.server.common.data.cf.configuration.CalculatedFieldConfiguration;
import org.thingsboard.server.common.data.cf.configuration.Output;
import org.thingsboard.server.common.data.cf.configuration.ReferencedEntityKey;
import org.thingsboard.server.common.data.id.CalculatedFieldId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.data.tenant.profile.DefaultTenantProfileConfiguration;
import org.thingsboard.server.common.util.ProtoUtils;
import org.thingsboard.server.dao.usagerecord.ApiLimitService;
import org.thingsboard.server.gen.transport.TransportProtos.CalculatedFieldTelemetryMsgProto;
import org.thingsboard.server.service.cf.ctx.CalculatedFieldEntityCtxId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.thingsboard.common.util.ExpressionFunctionsUtil.userDefinedFunctions;

/**
 * 计算字段上下文类，封装了单个计算字段在运行时的所有信息与状态。
 * <p>
 * 主要职责：
 * <ul>
 *   <li>存储计算字段的配置（参数、输出、表达式/脚本等）</li>
 *   <li>管理参数与实体键（主实体/关联实体）的映射关系</li>
 *   <li>初始化并持有表达式引擎（MVEL/exp4j）或 TBEL 脚本引擎</li>
 *   <li>判断传入的数据（遥测/属性）是否与当前计算字段关心的参数匹配</li>
 *   <li>记录租户级限制（如滚动窗口最大点数、状态大小等）</li>
 * </ul>
 * </p>
 * <p>
 * 每个计算字段实例对应一个 {@link CalculatedField}，并在运行时由 {@code CalculatedFieldService} 管理其生命周期。
 * </p>
 *
 * @author Thingsboard
 */
@Data
public class CalculatedFieldCtx {

    /** 原始计算字段实体对象 */
    private CalculatedField calculatedField;

    /** 计算字段唯一标识 */
    private CalculatedFieldId cfId;

    /** 所属租户 ID */
    private TenantId tenantId;

    /** 计算字段绑定的主实体 ID（通常为设备或资产） */
    private EntityId entityId;

    /** 计算字段类型：EXPRESSION（表达式）或 SCRIPT（脚本） */
    private CalculatedFieldType cfType;

    /** 参数定义映射：参数名 -> 参数配置（包括引用实体、引用键、类型等） */
    private final Map<String, Argument> arguments;

    /** 主实体参数映射：引用键（如遥测键、属性键名） -> 参数名 */
    private final Map<ReferencedEntityKey, String> mainEntityArguments;

    /** 关联实体参数映射：实体ID -> (引用键 -> 参数名) */
    private final Map<EntityId, Map<ReferencedEntityKey, String>> linkedEntityArguments;

    /** 所有参数的名称列表（顺序由配置决定） */
    private final List<String> argNames;

    /** 计算字段的输出定义（包含输出键和类型） */
    private Output output;

    /** 计算表达式或脚本内容 */
    private String expression;

    /** TBEL 脚本调用服务（仅脚本类型使用） */
    private TbelInvokeService tbelInvokeService;

    /** 脚本引擎封装（仅脚本类型使用，实际是TbelInvokeService包装） */
    private CalculatedFieldScriptEngine calculatedFieldScriptEngine;

    /** 线程本地表达式对象（仅表达式类型使用，exp4j 实现） */
    private ThreadLocal<Expression> customExpression;

    /** 上下文是否已成功初始化 */
    private boolean initialized;

    /** 租户级限制：每个滚动参数可存储的最大数据点数 */
    private long maxDataPointsPerRollingArg;

    /** 租户级限制：计算字段状态的最大内存占用（字节） */
    private long maxStateSize;

    /** 租户级限制：单个单值参数的最大大小（字节） */
    private long maxSingleValueArgumentSize;

    /**
     * 构造计算字段上下文。
     *
     * @param calculatedField    计算字段实体
     * @param tbelInvokeService  TBEL 调用服务（可为 null，表示禁用脚本）
     * @param apiLimitService    租户限流服务，用于获取租户级限制
     */
    public CalculatedFieldCtx(CalculatedField calculatedField, TbelInvokeService tbelInvokeService, ApiLimitService apiLimitService) {
        this.calculatedField = calculatedField;

        this.cfId = calculatedField.getId();
        this.tenantId = calculatedField.getTenantId();
        this.entityId = calculatedField.getEntityId();
        this.cfType = calculatedField.getType();
        CalculatedFieldConfiguration configuration = calculatedField.getConfiguration();
        this.arguments = configuration.getArguments();
        this.mainEntityArguments = new HashMap<>();
        this.linkedEntityArguments = new HashMap<>();
        // 遍历所有参数，根据引用实体 ID 将参数归类到主实体映射或关联实体映射中
        for (Map.Entry<String, Argument> entry : arguments.entrySet()) {
            var refId = entry.getValue().getRefEntityId();
            var refKey = entry.getValue().getRefEntityKey();
            if (refId == null || refId.equals(calculatedField.getEntityId())) {
                mainEntityArguments.put(refKey, entry.getKey());
            } else {
                linkedEntityArguments.computeIfAbsent(refId, key -> new HashMap<>()).put(refKey, entry.getKey());
            }
        }
        this.argNames = new ArrayList<>(arguments.keySet());
        this.output = configuration.getOutput();
        this.expression = configuration.getExpression();
        this.tbelInvokeService = tbelInvokeService;

        // 从租户配置中读取限制
        this.maxDataPointsPerRollingArg = apiLimitService.getLimit(tenantId, DefaultTenantProfileConfiguration::getMaxDataPointsPerRollingArg);
        this.maxStateSize = apiLimitService.getLimit(tenantId, DefaultTenantProfileConfiguration::getMaxStateSizeInKBytes) * 1024;
        this.maxSingleValueArgumentSize = apiLimitService.getLimit(tenantId, DefaultTenantProfileConfiguration::getMaxSingleValueArgumentSizeInKBytes) * 1024;
    }

    /**
     * 初始化计算字段的执行引擎。
     * <p>
     * 根据类型创建对应的引擎：
     * <ul>
     *   <li>SCRIPT：初始化 TBEL 脚本引擎</li>
     *   <li>EXPRESSION：编译 MVEL 表达式并准备 exp4j 表达式（线程本地）</li>
     * </ul>
     * 初始化成功后 {@code initialized} 置为 true。
     *
     * @throws RuntimeException 如果表达式语法错误或脚本引擎不可用
     */
    public void init() {
        if (CalculatedFieldType.SCRIPT.equals(cfType)) {
            try {
                // 如果计算字段是脚本类型，则初始化执行引擎
                this.calculatedFieldScriptEngine = initEngine(tenantId, expression, tbelInvokeService);
                initialized = true;
            } catch (Exception e) {
                throw new RuntimeException("Failed to init calculated field ctx. Invalid expression syntax.", e);
            }
        } else {
            // 如果计算字段是表达式，则初始化表达式对象
            if (isValidExpression(expression)) {
                this.customExpression = ThreadLocal.withInitial(() ->
                        new ExpressionBuilder(expression)
                                .functions(userDefinedFunctions)
                                .implicitMultiplication(true)
                                .variables(this.arguments.keySet())
                                .build()
                );
                initialized = true;
            } else {
                throw new RuntimeException("Failed to init calculated field ctx. Invalid expression syntax.");
            }
        }
    }

    /**
     * 停止计算字段上下文，释放引擎资源。
     * <p>
     * 脚本引擎调用 {@code destroy()}，表达式引擎清除线程本地变量。
     * </p>
     */
    public void stop() {
        if (calculatedFieldScriptEngine != null) {
            calculatedFieldScriptEngine.destroy();
        }
        if (customExpression != null) {
            customExpression.remove();
        }
    }

    /**
     * 初始化 TBEL 脚本引擎。
     *
     * @param tenantId           租户 ID
     * @param expression         脚本内容
     * @param tbelInvokeService  TBEL 调用服务
     * @return 脚本引擎封装实例
     * @throws IllegalArgumentException 如果 TBEL 服务不可用
     */
    private CalculatedFieldScriptEngine initEngine(TenantId tenantId, String expression, TbelInvokeService tbelInvokeService) {
        if (tbelInvokeService == null) {
            throw new IllegalArgumentException("TBEL script engine is disabled!");
        }

        List<String> ctxAndArgNames = new ArrayList<>(argNames.size() + 1);
        ctxAndArgNames.add("ctx"); // 脚本中可通过 ctx 访问上下文
        ctxAndArgNames.addAll(argNames);
        return new CalculatedFieldTbelScriptEngine(
                tenantId,
                tbelInvokeService,
                expression,
                ctxAndArgNames.toArray(String[]::new)
        );
    }

    /**
     * 使用 MVEL 验证表达式语法是否有效。
     *
     * @param expression 表达式字符串
     * @return true 如果语法正确，否则 false
     */
    private boolean isValidExpression(String expression) {
        try {
            MVEL.compileExpression(expression);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // ======================== 匹配方法 ==========================
    // 这些方法用于判断传入的数据（属性或遥测）是否与当前计算字段的参数相关，
    // 从而决定是否需要触发重新计算。

    /**
     * 检查主实体的属性更新是否匹配当前计算字段关心的主实体属性参数。
     *
     * @param values 更新的属性键值对列表
     * @param scope  属性作用域
     * @return true 如果至少有一个更新的属性被当前计算字段引用
     */
    public boolean matches(List<AttributeKvEntry> values, AttributeScope scope) {
        return matchesAttributes(mainEntityArguments, values, scope);
    }

    /**
     * 检查关联实体的属性更新是否匹配当前计算字段关心的关联实体属性参数。
     *
     * @param entityId 关联实体 ID
     * @param values   更新的属性键值对列表
     * @param scope    属性作用域
     * @return true 如果该实体的更新中有被当前计算字段引用的属性
     */
    public boolean linkMatches(EntityId entityId, List<AttributeKvEntry> values, AttributeScope scope) {
        var map = linkedEntityArguments.get(entityId);
        return map != null && matchesAttributes(map, values, scope);
    }

    /**
     * 检查主实体的遥测更新是否匹配当前计算字段关心的主实体遥测参数。
     *
     * @param values 更新的遥测键值对列表（带时间戳）
     * @return true 如果至少有一个更新的遥测键被当前计算字段引用
     */
    public boolean matches(List<TsKvEntry> values) {
        return matchesTimeSeries(mainEntityArguments, values);
    }

    /**
     * 检查关联实体的遥测更新是否匹配当前计算字段关心的关联实体遥测参数。
     *
     * @param entityId 关联实体 ID
     * @param values   更新的遥测键值对列表
     * @return true 如果该实体的更新中有被当前计算字段引用的遥测键
     */
    public boolean linkMatches(EntityId entityId, List<TsKvEntry> values) {
        var map = linkedEntityArguments.get(entityId);
        return map != null && matchesTimeSeries(map, values);
    }

    /**
     * 通用属性匹配逻辑。
     *
     * @param argMap 参数映射（引用键 -> 参数名）
     * @param values 更新的属性列表
     * @param scope  属性作用域
     * @return true 如果存在匹配
     */
    private boolean matchesAttributes(Map<ReferencedEntityKey, String> argMap, List<AttributeKvEntry> values, AttributeScope scope) {
        for (AttributeKvEntry attrKv : values) {
            ReferencedEntityKey attrKey = new ReferencedEntityKey(attrKv.getKey(), ArgumentType.ATTRIBUTE, scope);
            if (argMap.containsKey(attrKey)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 通用遥测匹配逻辑（同时检查最新值参数和滚动参数）。
     *
     * @param argMap 参数映射（引用键 -> 参数名）
     * @param values 更新的遥测列表
     * @return true 如果存在匹配
     */
    private boolean matchesTimeSeries(Map<ReferencedEntityKey, String> argMap, List<TsKvEntry> values) {
        for (TsKvEntry tsKv : values) {
            ReferencedEntityKey latestKey = new ReferencedEntityKey(tsKv.getKey(), ArgumentType.TS_LATEST, null);
            if (argMap.containsKey(latestKey)) {
                return true;
            }
            ReferencedEntityKey rollingKey = new ReferencedEntityKey(tsKv.getKey(), ArgumentType.TS_ROLLING, null);
            if (argMap.containsKey(rollingKey)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 根据主实体属性键列表检查是否匹配（用于处理删除操作）。
     *
     * @param keys  属性键列表
     * @param scope 作用域
     * @return true 如果存在匹配
     */
    public boolean matchesKeys(List<String> keys, AttributeScope scope) {
        return matchesAttributesKeys(mainEntityArguments, keys, scope);
    }

    /**
     * 根据主实体遥测键列表检查是否匹配（用于处理删除操作）。
     *
     * @param keys 遥测键列表
     * @return true 如果存在匹配
     */
    public boolean matchesKeys(List<String> keys) {
        return matchesTimeSeriesKeys(mainEntityArguments, keys);
    }

    /**
     * 通用属性键匹配逻辑。
     *
     * @param argMap 参数映射
     * @param keys   属性键列表
     * @param scope  作用域
     * @return true 如果存在匹配
     */
    private boolean matchesAttributesKeys(Map<ReferencedEntityKey, String> argMap, List<String> keys, AttributeScope scope) {
        for (String key : keys) {
            ReferencedEntityKey attrKey = new ReferencedEntityKey(key, ArgumentType.ATTRIBUTE, scope);
            if (argMap.containsKey(attrKey)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 通用遥测键匹配逻辑（同时检查最新值和滚动参数）。
     *
     * @param argMap 参数映射
     * @param keys   遥测键列表
     * @return true 如果存在匹配
     */
    private boolean matchesTimeSeriesKeys(Map<ReferencedEntityKey, String> argMap, List<String> keys) {
        for (String key : keys) {
            ReferencedEntityKey latestKey = new ReferencedEntityKey(key, ArgumentType.TS_LATEST, null);
            if (argMap.containsKey(latestKey)) {
                return true;
            }
            ReferencedEntityKey rollingKey = new ReferencedEntityKey(key, ArgumentType.TS_ROLLING, null);
            if (argMap.containsKey(rollingKey)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 检查关联实体的属性键是否匹配。
     *
     * @param entityId 关联实体 ID
     * @param keys     属性键列表
     * @param scope    作用域
     * @return true 如果存在匹配
     */
    public boolean linkMatchesAttrKeys(EntityId entityId, List<String> keys, AttributeScope scope) {
        var map = linkedEntityArguments.get(entityId);
        return map != null && matchesAttributesKeys(map, keys, scope);
    }

    /**
     * 检查关联实体的遥测键是否匹配。
     *
     * @param entityId 关联实体 ID
     * @param keys     遥测键列表
     * @return true 如果存在匹配
     */
    public boolean linkMatchesTsKeys(EntityId entityId, List<String> keys) {
        var map = linkedEntityArguments.get(entityId);
        return map != null && matchesTimeSeriesKeys(map, keys);
    }

    /**
     * 根据传入的 Protobuf 消息判断关联实体的更新是否匹配。
     * <p>
     * 消息可能包含遥测数据、属性数据或删除键列表，分别进行相应匹配。
     * </p>
     *
     * @param entityId 关联实体 ID
     * @param proto    Telemetry 消息 Protobuf
     * @return true 如果该更新影响当前计算字段
     */
    public boolean linkMatches(EntityId entityId, CalculatedFieldTelemetryMsgProto proto) {
        if (!proto.getTsDataList().isEmpty()) {
            List<TsKvEntry> updatedTelemetry = proto.getTsDataList().stream()
                    .map(ProtoUtils::fromProto)
                    .toList();
            return linkMatches(entityId, updatedTelemetry);
        } else if (!proto.getAttrDataList().isEmpty()) {
            AttributeScope scope = AttributeScope.valueOf(proto.getScope().name());
            List<AttributeKvEntry> updatedTelemetry = proto.getAttrDataList().stream()
                    .map(ProtoUtils::fromProto)
                    .toList();
            return linkMatches(entityId, updatedTelemetry, scope);
        } else if (!proto.getRemovedTsKeysList().isEmpty()) {
            return linkMatchesTsKeys(entityId, proto.getRemovedTsKeysList());
        } else {
            return linkMatchesAttrKeys(entityId, proto.getRemovedAttrKeysList(), AttributeScope.valueOf(proto.getScope().name()));
        }
    }

    /**
     * 生成此上下文对应的标识符，用于状态存储和查找。
     *
     * @return 组合了租户、计算字段 ID 和实体 ID 的上下文 ID
     */
    public CalculatedFieldEntityCtxId toCalculatedFieldEntityCtxId() {
        return new CalculatedFieldEntityCtxId(tenantId, cfId, entityId);
    }

    /**
     * 判断另一个上下文是否有“非状态性”的重大变化（表达式或输出改变）。
     * <p>
     * 此类变化不会影响参数结构，但会影响计算结果，因此需要重新初始化引擎。
     * </p>
     *
     * @param other 另一个上下文
     * @return true 如果表达式或输出发生变化
     */
    public boolean hasOtherSignificantChanges(CalculatedFieldCtx other) {
        boolean expressionChanged = !expression.equals(other.expression);
        boolean outputChanged = !output.equals(other.output);
        return expressionChanged || outputChanged;
    }

    /**
     * 判断另一个上下文是否有影响“状态”的变化（类型或参数映射改变）。
     * <p>
     * 此类变化会导致状态结构改变，需要丢弃原有状态。
     * </p>
     *
     * @param other 另一个上下文
     * @return true 如果类型或参数发生变化
     */
    public boolean hasStateChanges(CalculatedFieldCtx other) {
        boolean typeChanged = !cfType.equals(other.cfType);
        boolean argumentsChanged = !arguments.equals(other.arguments);
        return typeChanged || argumentsChanged;
    }

    /**
     * 获取状态大小超出限制时的错误信息。
     *
     * @return 格式化的错误字符串
     */
    public String getSizeExceedsLimitMessage() {
        return "Failed to init CF state. State size exceeds limit of " + (maxStateSize / 1024) + "Kb!";
    }

}
