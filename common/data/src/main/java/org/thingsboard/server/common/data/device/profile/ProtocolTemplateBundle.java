/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.device.profile;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 租户级协议模板包：一条数据库记录对应一个包；{@link #id} 为平台生成的 UUID，
 * 设备 TCP 配置中 {@link ProtocolTemplateTransportTcpDataConfiguration#getProtocolTemplateBundleId()} 引用该值。
 * 包内帧模板的 {@link ProtocolTemplateDefinition#getId()} 为逻辑模板标识（命令通过 templateId 关联），与包 id 不同。
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProtocolTemplateBundle implements Serializable {

    private String id;
    /** 创建时间（毫秒），来自库表；列表展示与排序用 */
    private Long createdTime;
    private String name;

    @JsonProperty("protocolTemplates")
    @JsonAlias({"monitoringTemplates"})
    private List<ProtocolTemplateDefinition> protocolTemplates = new ArrayList<>();

    @JsonProperty("protocolCommands")
    @JsonAlias({"monitoringCommands"})
    private List<ProtocolTemplateCommandDefinition> protocolCommands = new ArrayList<>();
}
