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
import lombok.AllArgsConstructor;
import lombok.Data;
import org.thingsboard.server.common.data.AttributeScope;

/**
 * 引用实体键类，用于定位实体上的具体数据点。
 * <p>
 * 该类的实例通过组合 {@code key}（数据点名称）、{@code type}（数据类型）和
 * {@code scope}（当类型为属性时指定属性作用域）来唯一标识一个数据来源。
 * </p>
 * <p>
 * 例如：
 * <ul>
 *   <li>最新遥测：key="temperature", type=TS_LATEST, scope=null</li>
 *   <li>滚动窗口遥测：key="humidity", type=TS_ROLLING, scope=null</li>
 *   <li>服务器属性：key="firmware", type=ATTRIBUTE, scope=SERVER_SCOPE</li>
 * </ul>
 * </p>
 *
 * @author Thingsboard
 * @see ArgumentType
 * @see AttributeScope
 */
@Data
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ReferencedEntityKey {

    /**
     * 数据点的键名，例如遥测名称 "temperature" 或属性名称 "serialNumber"。
     */
    private String key;

    /**
     * 参数类型，决定数据来源是属性、最新遥测还是滚动窗口遥测。
     */
    private ArgumentType type;

    /**
     * 属性作用域。
     * <p>
     * 仅当 {@code type} 为 {@link ArgumentType#ATTRIBUTE} 时有效。
     * 指定从哪个属性作用域（如客户端、服务器、共享）获取数据。
     * </p>
     */
    private AttributeScope scope;

}
