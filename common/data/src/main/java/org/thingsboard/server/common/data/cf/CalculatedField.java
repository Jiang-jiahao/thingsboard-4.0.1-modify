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
package org.thingsboard.server.common.data.cf;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.thingsboard.server.common.data.BaseData;
import org.thingsboard.server.common.data.HasDebugSettings;
import org.thingsboard.server.common.data.HasName;
import org.thingsboard.server.common.data.HasTenantId;
import org.thingsboard.server.common.data.HasVersion;
import org.thingsboard.server.common.data.cf.configuration.CalculatedFieldConfiguration;
import org.thingsboard.server.common.data.cf.configuration.SimpleCalculatedFieldConfiguration;
import org.thingsboard.server.common.data.debug.DebugSettings;
import org.thingsboard.server.common.data.id.CalculatedFieldId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.validation.Length;
import org.thingsboard.server.common.data.validation.NoXss;

import java.io.Serial;

/**
 * 计算字段实体类，表示一个在 ThingsBoard 中定义的计算字段。
 * <p>
 * 计算字段允许用户基于设备遥测、属性等数据，通过表达式或脚本实时计算新的数据点。
 * 该类封装了计算字段的核心属性，包括所属租户、绑定的实体（设备/资产）、计算类型、
 * 名称、调试设置、配置版本以及具体的计算配置。
 * </p>
 * <p>
 * 继承自 {@link BaseData}，拥有唯一的 {@link CalculatedFieldId} 和创建时间。
 * 实现了多个接口以提供通用的访问方法：{@link HasName}、{@link HasTenantId}、
 * {@link HasVersion}（用于乐观锁）和 {@link HasDebugSettings}（用于调试）。
 * </p>
 *
 * @author Thingsboard
 * @see CalculatedFieldConfiguration
 * @see SimpleCalculatedFieldConfiguration
 */
@Schema
@Data
@EqualsAndHashCode(callSuper = true)
public class CalculatedField extends BaseData<CalculatedFieldId> implements HasName, HasTenantId, HasVersion, HasDebugSettings {

    @Serial
    private static final long serialVersionUID = 4491966747773381420L;

    /** 所属租户 ID */
    private TenantId tenantId;

    /** 计算字段绑定的实体 ID（通常为设备或资产） */
    private EntityId entityId;

    /** 计算字段类型：表达式（EXPRESSION）或脚本（SCRIPT） */
    @NoXss
    @Length(fieldName = "type")
    private CalculatedFieldType type;

    /** 用户自定义的计算字段名称 */
    @NoXss
    @Length(fieldName = "name")
    @Schema(description = "User defined name of the calculated field.")
    private String name;

    /**
     * 是否启用调试模式。
     * @deprecated 自某版本起已废弃，推荐使用 {@link #debugSettings} 进行更精细的调试配置。
     * 为了向后兼容，序列化时忽略该字段，反序列化时仍支持设置。
     */
    @Deprecated
    @Schema(description = "Enable/disable debug. ", example = "false", deprecated = true)
    private boolean debugMode;

    /** 调试设置对象，包含采样率、调试有效期等配置 */
    @Schema(description = "Debug settings object.")
    private DebugSettings debugSettings;

    /** 计算字段配置的版本号，用于跟踪配置变更 */
    @Schema(description = "Version of calculated field configuration.", example = "0")
    private int configurationVersion;

    /** 计算字段的具体配置，包含参数定义、输出定义、表达式/脚本内容等 */
    @Schema(implementation = SimpleCalculatedFieldConfiguration.class)
    private CalculatedFieldConfiguration configuration;

    /** 实体版本号，用于乐观锁并发控制 */
    @Getter
    @Setter
    private Long version;

    public CalculatedField() {
        super();
    }

    public CalculatedField(CalculatedFieldId id) {
        super(id);
    }

    public CalculatedField(TenantId tenantId, EntityId entityId, CalculatedFieldType type, String name, int configurationVersion, CalculatedFieldConfiguration configuration, Long version) {
        this.tenantId = tenantId;
        this.entityId = entityId;
        this.type = type;
        this.name = name;
        this.configurationVersion = configurationVersion;
        this.configuration = configuration;
        this.version = version;
    }

    public CalculatedField(CalculatedField calculatedField) {
        super(calculatedField);
        this.tenantId = calculatedField.tenantId;
        this.entityId = calculatedField.entityId;
        this.type = calculatedField.type;
        this.name = calculatedField.name;
        this.debugMode = calculatedField.debugMode;
        this.debugSettings = calculatedField.debugSettings;
        this.configurationVersion = calculatedField.configurationVersion;
        this.configuration = calculatedField.configuration;
        this.version = calculatedField.version;
    }


    /**
     * 获取计算字段的唯一标识。
     *
     * @return 计算字段 ID
     */
    @Schema(description = "JSON object with the Calculated Field Id. Referencing non-existing Calculated Field Id will cause error.")
    @Override
    public CalculatedFieldId getId() {
        return super.getId();
    }

    /**
     * 获取计算字段的创建时间戳（毫秒）。
     *
     * @return 创建时间
     */
    @Schema(description = "Timestamp of the calculated field creation, in milliseconds", example = "1609459200000", accessMode = Schema.AccessMode.READ_ONLY)
    @Override
    public long getCreatedTime() {
        return super.getCreatedTime();
    }

    /**
     * 获取调试模式开关（仅用于向后兼容的序列化）。
     * 实际调试配置应通过 {@link #getDebugSettings()} 获取。
     *
     * @return 是否启用调试模式（废弃）
     */
    // Getter is ignored for serialization
    @JsonIgnore
    public boolean isDebugMode() {
        return debugMode;
    }

    /**
     * 设置调试模式开关（仅用于向后兼容的反序列化）。
     *
     * @param debugMode 是否启用调试模式
     */
    // Setter is annotated for deserialization
    @JsonSetter
    public void setDebugMode(boolean debugMode) {
        this.debugMode = debugMode;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("CalculatedField[")
                .append("tenantId=").append(tenantId)
                .append(", entityId=").append(entityId)
                .append(", type='").append(type)
                .append(", name='").append(name)
                .append(", configurationVersion=").append(configurationVersion)
                .append(", configuration=").append(configuration)
                .append(", version=").append(version)
                .append(", createdTime=").append(createdTime)
                .append(", id=").append(id).append(']')
                .toString();
    }

}
