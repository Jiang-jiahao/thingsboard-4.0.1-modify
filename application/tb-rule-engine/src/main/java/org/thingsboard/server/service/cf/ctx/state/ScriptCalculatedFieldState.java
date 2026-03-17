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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.script.api.tbel.TbelCfArg;
import org.thingsboard.script.api.tbel.TbelCfCtx;
import org.thingsboard.script.api.tbel.TbelCfSingleValueArg;
import org.thingsboard.server.common.data.cf.CalculatedFieldType;
import org.thingsboard.server.common.data.cf.configuration.Output;
import org.thingsboard.server.service.cf.CalculatedFieldResult;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * TBEL 脚本计算字段的状态实现。
 * <p>
 * 此类对应 {@link CalculatedFieldType#SCRIPT} 类型的计算字段，使用 TBEL 脚本引擎执行计算。
 * 它支持复杂的数据结构（如数组、对象），参数可以是单值或滚动窗口值，计算结果可以是任意 JSON 对象。
 * </p>
 *
 * @author Thingsboard
 * @see BaseCalculatedFieldState
 * @see CalculatedFieldType#SCRIPT
 */
@Data
@Slf4j
@NoArgsConstructor
public class ScriptCalculatedFieldState extends BaseCalculatedFieldState {

    /**
     * 使用必需的参数列表构造状态对象。
     *
     * @param requiredArguments 必需的参数名称列表
     */
    public ScriptCalculatedFieldState(List<String> requiredArguments) {
        super(requiredArguments);
    }

    /**
     * 获取计算字段类型，固定返回 {@link CalculatedFieldType#SCRIPT}。
     *
     * @return 计算字段类型
     */
    @Override
    public CalculatedFieldType getType() {
        return CalculatedFieldType.SCRIPT;
    }

    /**
     * 验证新条目：脚本类型允许任何参数类型（单值或滚动窗口），因此不做验证。
     *
     * @param newEntry 待验证的新参数条目
     */
    @Override
    protected void validateNewEntry(ArgumentEntry newEntry) {
        // 脚本类型不做额外验证
    }

    /**
     * 执行计算。
     * <p>
     * 步骤：
     * <ol>
     *   <li>构造 TBEL 脚本的参数列表，第一个元素为 {@link TbelCfCtx} 上下文对象，后续为各参数的值或对象。</li>
     *   <li>将每个参数条目转换为 {@link TbelCfArg} 对象，单值参数提取其值放入参数列表，滚动窗口参数直接放入对象。</li>
     *   <li>调用脚本引擎的 {@code executeJsonAsync} 方法异步执行脚本，返回 {@link JsonNode} 结果。</li>
     *   <li>将结果封装为 {@link CalculatedFieldResult} 返回。</li>
     * </ol>
     * </p>
     *
     * @param ctx 计算字段上下文，包含脚本引擎、参数名称列表、输出配置等信息
     * @return 包含计算结果的 {@link ListenableFuture}
     */
    @Override
    public ListenableFuture<CalculatedFieldResult> performCalculation(CalculatedFieldCtx ctx) {
        Map<String, TbelCfArg> arguments = new LinkedHashMap<>();
        List<Object> args = new ArrayList<>(ctx.getArgNames().size() + 1);
        args.add(new Object()); // first element is a ctx, but we will set it later;
        for (String argName : ctx.getArgNames()) {
            var arg = toTbelArgument(argName);
            arguments.put(argName, arg);
            if (arg instanceof TbelCfSingleValueArg svArg) {
                args.add(svArg.getValue());
            } else {
                args.add(arg);
            }
        }
        args.set(0, new TbelCfCtx(arguments));
        ListenableFuture<JsonNode> resultFuture = ctx.getCalculatedFieldScriptEngine().executeJsonAsync(args.toArray());
        Output output = ctx.getOutput();
        return Futures.transform(resultFuture,
                result -> new CalculatedFieldResult(output.getType(), output.getScope(), result),
                MoreExecutors.directExecutor()
        );
    }

    /**
     * 将内部存储的参数条目转换为 TBEL 脚本引擎可识别的 {@link TbelCfArg} 对象。
     *
     * @param key 参数名称
     * @return 对应的 TBEL 参数对象
     */
    private TbelCfArg toTbelArgument(String key) {
        return arguments.get(key).toTbelCfArg();
    }

}
