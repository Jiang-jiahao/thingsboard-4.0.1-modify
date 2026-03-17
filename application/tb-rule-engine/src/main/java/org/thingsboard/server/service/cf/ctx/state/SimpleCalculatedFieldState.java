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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.script.api.tbel.TbUtils;
import org.thingsboard.server.common.data.cf.CalculatedFieldType;
import org.thingsboard.server.common.data.cf.configuration.Output;
import org.thingsboard.server.common.data.kv.BasicKvEntry;
import org.thingsboard.server.service.cf.CalculatedFieldResult;

import java.util.List;
import java.util.Map;

/**
 * 简单表达式计算字段的状态实现。
 * <p>
 * 此类对应 {@link CalculatedFieldType#SIMPLE} 类型的计算字段，使用 exp4j 表达式引擎执行计算。
 * 它要求所有参数均为数值类型（可转换为 double），计算结果按输出配置进行格式化（保留小数位数或转换为整数）。
 * </p>
 *
 * @author Thingsboard
 * @see BaseCalculatedFieldState
 * @see CalculatedFieldType#SIMPLE
 */
@Data
@NoArgsConstructor
public class SimpleCalculatedFieldState extends BaseCalculatedFieldState {

    /**
     * 使用必需的参数列表构造状态对象。
     *
     * @param requiredArguments 必需的参数名称列表
     */
    public SimpleCalculatedFieldState(List<String> requiredArguments) {
        super(requiredArguments);
    }

    /**
     * 获取计算字段类型，固定返回 {@link CalculatedFieldType#SIMPLE}。
     *
     * @return 计算字段类型
     */
    @Override
    public CalculatedFieldType getType() {
        return CalculatedFieldType.SIMPLE;
    }

    /**
     * 验证新条目：简单表达式不支持滚动窗口参数。
     *
     * @param newEntry 待验证的新参数条目
     * @throws IllegalArgumentException 如果新条目是 {@link TsRollingArgumentEntry} 类型
     */
    @Override
    protected void validateNewEntry(ArgumentEntry newEntry) {
        if (newEntry instanceof TsRollingArgumentEntry) {
            throw new IllegalArgumentException("Rolling argument entry is not supported for simple calculated fields.");
        }
    }

    /**
     * 执行计算。
     * <p>
     * 步骤：
     * <ol>
     *   <li>从上下文获取线程本地的 exp4j {@code Expression} 对象。</li>
     *   <li>遍历当前状态中的所有参数条目，将其值转换为 double 并设置为表达式的变量。</li>
     *   <li>计算表达式结果（double）。</li>
     *   <li>根据输出配置格式化结果（保留小数、转换为整数或保持原样）。</li>
     *   <li>构建并返回 {@link CalculatedFieldResult}，包含输出类型、作用域和计算结果 JSON。</li>
     * </ol>
     * </p>
     *
     * @param ctx 计算字段上下文，包含表达式对象、输出配置等信息
     * @return 包含计算结果的 {@link ListenableFuture}（立即完成）
     */
    @Override
    public ListenableFuture<CalculatedFieldResult> performCalculation(CalculatedFieldCtx ctx) {
        var expr = ctx.getCustomExpression().get();

        for (Map.Entry<String, ArgumentEntry> entry : this.arguments.entrySet()) {
            try {
                BasicKvEntry kvEntry = ((SingleValueArgumentEntry) entry.getValue()).getKvEntryValue();
                expr.setVariable(entry.getKey(), Double.parseDouble(kvEntry.getValueAsString()));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Argument '" + entry.getKey() + "' is not a number.");
            }
        }

        double expressionResult = expr.evaluate();

        Output output = ctx.getOutput();
        Object result;
        Integer decimals = output.getDecimalsByDefault();
        if (decimals != null) {
            if (decimals.equals(0)) {
                result = TbUtils.toInt(expressionResult);
            } else {
                result = TbUtils.toFixed(expressionResult, decimals);
            }
        } else {
            result = expressionResult;
        }

        return Futures.immediateFuture(new CalculatedFieldResult(output.getType(), output.getScope(), JacksonUtil.valueToTree(Map.of(output.getName(), result))));
    }

}
