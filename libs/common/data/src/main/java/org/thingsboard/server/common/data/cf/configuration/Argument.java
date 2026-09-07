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

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import org.springframework.lang.Nullable;
import org.thingsboard.server.common.data.id.EntityId;

/**
 * 计算字段参数定义类。
 * <p>
 * 每个参数代表计算字段的一个输入来源，可以来自主实体的数据，也可以来自其他关联实体的数据。
 * 参数通过引用实体 ID、引用实体键以及可选的默认值、限制和时间窗口来描述其数据源。
 * </p>
 * <p>
 * 参数类型（最新遥测、滚动窗口遥测、属性）由 {@link ReferencedEntityKey#getType()} 决定。
 * 对于滚动窗口参数，{@code limit} 和 {@code timeWindow} 用于限定窗口的大小和时间范围。
 * </p>
 *
 * @author Thingsboard
 * @see ReferencedEntityKey
 * @see ArgumentType
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Argument {

    /**
     * 引用实体 ID。
     * <p>
     * 如果为 {@code null} 或等于计算字段所属的主实体 ID，则表示参数引用的是主实体的数据；
     * 否则表示引用的是其他关联实体的数据。
     * </p>
     */
    @Nullable
    private EntityId refEntityId;

    /**
     * 引用实体键，包含键名、数据类型（属性/最新遥测/滚动遥测）以及属性作用域。
     * 用于唯一标识被引用实体上的某个具体数据点。
     */
    private ReferencedEntityKey refEntityKey;

    /**
     * 默认值。
     * <p>
     * 当被引用的数据点不存在时，将使用此默认值作为参数输入。
     * 默认值的格式应与参数期望的数据类型相匹配。
     * </p>
     */
    private String defaultValue;

    /**
     * 滚动窗口参数的数据点数量限制。
     * <p>
     * 仅当 {@code refEntityKey.type} 为 {@link ArgumentType#TS_ROLLING} 时有效。
     * 指定窗口内保留的最大数据点数，超过限制时可能会进行数据裁剪或聚合。
     * </p>
     */
    private Integer limit;

    /**
     * 滚动窗口参数的时间窗口（毫秒）。
     * <p>
     * 仅当 {@code refEntityKey.type} 为 {@link ArgumentType#TS_ROLLING} 时有效。
     * 定义窗口的时间跨度，例如过去 60000 毫秒（1 分钟）的数据。
     * </p>
     */
    private Long timeWindow;

}
