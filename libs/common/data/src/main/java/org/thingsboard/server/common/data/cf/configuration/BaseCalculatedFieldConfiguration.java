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
package org.thingsboard.server.common.data.cf.configuration;

import lombok.Data;
import org.thingsboard.server.common.data.cf.CalculatedFieldLink;
import org.thingsboard.server.common.data.id.CalculatedFieldId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 计算字段配置的抽象基类，实现了 {@link CalculatedFieldConfiguration} 接口。
 * <p>
 * 该类封装了计算字段的通用配置结构，包括参数映射、表达式/脚本内容以及输出定义。
 * 同时提供了获取所有引用实体 ID 以及构建计算字段链接的默认实现。
 * </p>
 *
 * @author Thingsboard
 * @see CalculatedFieldConfiguration
 * @see Argument
 * @see Output
 */
@Data
public abstract class BaseCalculatedFieldConfiguration implements CalculatedFieldConfiguration {

    /**
     * 参数名称到参数定义的映射。
     * 每个参数定义了计算字段的一个输入来源（遥测、属性或来自其他实体的数据）。
     */
    protected Map<String, Argument> arguments;

    /**
     * 计算表达式或脚本内容。
     * 对于表达式类型（EXPRESSION），为 MVEL/exp4j 表达式；对于脚本类型（SCRIPT），为 TBEL 脚本。
     */
    protected String expression;

    /**
     * 输出定义，指定计算结果的键名和数据类型。
     */
    protected Output output;

    /**
     * 获取此计算字段配置中所有被引用的实体 ID。
     * <p>
     * 遍历所有参数，收集那些指定了引用实体 ID（即从其他实体获取数据）的参数对应的实体 ID。
     * 参数如果未设置引用实体 ID（即引用的是主实体本身），则不包含在结果中。
     * </p>
     *
     * @return 被引用实体 ID 列表，不包含重复项（由调用者保证无重复，但实际可能有重复，此处直接收集）
     */
    @Override
    public List<EntityId> getReferencedEntities() {
        return arguments.values().stream()
                .map(Argument::getRefEntityId)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * 构建此计算字段与所有被引用实体之间的链接对象列表。
     * <p>
     * 对于每个被引用的实体（且不是计算字段自身所属的实体），创建一个 {@link CalculatedFieldLink}，
     * 表示该实体依赖于此计算字段。这些链接用于建立实体间的依赖关系，以便在实体更新时触发相关计算字段的重新计算。
     * </p>
     *
     * @param tenantId          租户 ID
     * @param cfEntityId        计算字段所属的主实体 ID（通常为设备或资产）
     * @param calculatedFieldId 计算字段 ID
     * @return 计算字段链接列表
     */
    @Override
    public List<CalculatedFieldLink> buildCalculatedFieldLinks(TenantId tenantId, EntityId cfEntityId, CalculatedFieldId calculatedFieldId) {
        return getReferencedEntities().stream()
                .filter(referencedEntity -> !referencedEntity.equals(cfEntityId))
                .map(referencedEntityId -> buildCalculatedFieldLink(tenantId, referencedEntityId, calculatedFieldId))
                .collect(Collectors.toList());
    }

    /**
     * 构建单个计算字段链接对象。
     *
     * @param tenantId           租户 ID
     * @param referencedEntityId 被引用的实体 ID
     * @param calculatedFieldId  计算字段 ID
     * @return 计算字段链接实例
     */
    @Override
    public CalculatedFieldLink buildCalculatedFieldLink(TenantId tenantId, EntityId referencedEntityId, CalculatedFieldId calculatedFieldId) {
        CalculatedFieldLink link = new CalculatedFieldLink();
        link.setTenantId(tenantId);
        link.setEntityId(referencedEntityId);
        link.setCalculatedFieldId(calculatedFieldId);
        return link;
    }

}
