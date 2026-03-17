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

import lombok.AllArgsConstructor;
import lombok.Data;
import org.thingsboard.server.service.cf.ctx.CalculatedFieldEntityCtxId;
import org.thingsboard.server.utils.CalculatedFieldUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.thingsboard.server.utils.CalculatedFieldUtils.toSingleValueArgumentProto;

/**
 * 计算字段状态的抽象基类。
 * <p>
 * 该类封装了计算字段运行时的通用状态管理逻辑，包括参数的存储、状态更新、就绪性检查以及大小限制校验。
 * 具体的计算字段类型（如简单表达式或脚本）应继承此类并实现各自的 {@link #performCalculation} 和 {@link #validateNewEntry} 方法。
 * </p>
 *
 * @author Thingsboard
 * @see SimpleCalculatedFieldState
 * @see ScriptCalculatedFieldState
 */
@Data
@AllArgsConstructor
public abstract class BaseCalculatedFieldState implements CalculatedFieldState {

    /** 计算字段所需参数的名称列表（顺序由配置决定） */
    protected List<String> requiredArguments;

    /** 参数名称到参数条目（ArgumentEntry）的映射，存储当前状态中的参数值 */
    protected Map<String, ArgumentEntry> arguments;

    /** 状态大小是否已超过租户限制的标志 */
    protected boolean sizeExceedsLimit;

    /**
     * 使用必需的参数列表构造状态对象，初始化空的参数映射。
     *
     * @param requiredArguments 必需的参数名称列表
     */
    public BaseCalculatedFieldState(List<String> requiredArguments) {
        this.requiredArguments = requiredArguments;
        this.arguments = new HashMap<>();
    }

    /**
     * 默认构造函数，初始化空列表和空映射。
     */
    public BaseCalculatedFieldState() {
        this(new ArrayList<>(), new HashMap<>(), false);
    }

    /**
     * 更新状态中的参数值。
     * <p>
     * 遍历传入的参数值映射，对于每个参数：
     * <ul>
     *   <li>如果参数不存在于当前状态中，或者新条目标记为强制重置，则直接替换并标记状态已更新。</li>
     *   <li>否则调用现有条目的 {@code updateEntry} 方法进行合并更新，并检查是否发生了变化。</li>
     * </ul>
     * 在更新前会调用 {@link #checkArgumentSize} 检查单个参数的大小是否超出限制。
     * </p>
     *
     * @param ctx            计算字段上下文，用于获取大小限制等配置
     * @param argumentValues 参数名称到新参数条目的映射
     * @return 如果状态发生了实际变化则返回 true，否则返回 false
     */
    @Override
    public boolean updateState(CalculatedFieldCtx ctx, Map<String, ArgumentEntry> argumentValues) {
        if (arguments == null) {
            arguments = new HashMap<>();
        }

        boolean stateUpdated = false;

        for (Map.Entry<String, ArgumentEntry> entry : argumentValues.entrySet()) {
            String key = entry.getKey();
            ArgumentEntry newEntry = entry.getValue();

            checkArgumentSize(key, newEntry, ctx);

            ArgumentEntry existingEntry = arguments.get(key);

            if (existingEntry == null || newEntry.isForceResetPrevious()) {
                validateNewEntry(newEntry);
                arguments.put(key, newEntry);
                stateUpdated = true;
            } else {
                stateUpdated = existingEntry.updateEntry(newEntry);
            }
        }

        return stateUpdated;
    }

    /**
     * 检查状态是否已就绪，即所有必需的参数都存在且非空。
     *
     * @return 如果所有必需参数都已就绪则返回 true，否则返回 false
     */
    @Override
    public boolean isReady() {
        return arguments.keySet().containsAll(requiredArguments) &&
                arguments.values().stream().noneMatch(ArgumentEntry::isEmpty);
    }

    /**
     * 检查状态大小是否超过租户限制，如果超过则清空状态并标记 {@code sizeExceedsLimit}。
     *
     * @param ctxId        计算字段实体上下文 ID，用于日志或调试
     * @param maxStateSize 最大允许的状态大小（字节）
     */
    @Override
    public void checkStateSize(CalculatedFieldEntityCtxId ctxId, long maxStateSize) {
        if (!sizeExceedsLimit && maxStateSize > 0 && CalculatedFieldUtils.toProto(ctxId, this).getSerializedSize() > maxStateSize) {
            arguments.clear();
            sizeExceedsLimit = true;
        }
    }

    /**
     * 检查单个参数条目的大小是否超过租户限制。
     * <p>
     * 对于滚动窗口参数（{@link TsRollingArgumentEntry}）不做大小检查。
     * 对于单值参数（{@link SingleValueArgumentEntry}），将其序列化为 Protobuf 后检查大小。
     * </p>
     *
     * @param name  参数名称
     * @param entry 参数条目
     * @param ctx   计算字段上下文，用于获取大小限制
     * @throws IllegalArgumentException 如果单值参数大小超过限制
     */
    @Override
    public void checkArgumentSize(String name, ArgumentEntry entry, CalculatedFieldCtx ctx) {
        if (entry instanceof TsRollingArgumentEntry) {
            return;
        }
        if (entry instanceof SingleValueArgumentEntry singleValueArgumentEntry) {
            if (ctx.getMaxSingleValueArgumentSize() > 0 && toSingleValueArgumentProto(name, singleValueArgumentEntry).getSerializedSize() > ctx.getMaxSingleValueArgumentSize()) {
                throw new IllegalArgumentException("Single value size exceeds the maximum allowed limit. The argument will not be used for calculation.");
            }
        }
    }

    /**
     * 验证新条目是否合法。
     * <p>
     * 子类可以实现具体的验证逻辑，例如禁止某些条目类型。
     * </p>
     *
     * @param newEntry 待验证的新参数条目
     */
    protected abstract void validateNewEntry(ArgumentEntry newEntry);

}
