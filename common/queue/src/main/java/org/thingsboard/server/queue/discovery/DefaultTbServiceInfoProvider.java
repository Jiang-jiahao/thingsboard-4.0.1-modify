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
package org.thingsboard.server.queue.discovery;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.TbTransportService;
import org.thingsboard.server.common.data.util.CollectionsUtil;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.gen.transport.TransportProtos.ServiceInfo;
import org.thingsboard.server.queue.edqs.EdqsConfig;
import org.thingsboard.server.queue.util.AfterContextReady;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.thingsboard.common.util.SystemUtil.*;


@Component
@Slf4j
public class DefaultTbServiceInfoProvider implements TbServiceInfoProvider {

    // 服务唯一标识符（可从配置注入，默认为主机名）
    @Getter
    @Value("${service.id:#{null}}")
    private String serviceId;

    // 服务类型（默认为monolith单体架构）
    @Getter
    @Value("${service.type:monolith}")
    private String serviceType;

    // 本服务实例分配的租户档案ID列表（仅对规则引擎服务有效），也就是这个实例只会处理assignedTenantProfiles包含的租户
    @Getter
    @Value("${service.rule_engine.assigned_tenant_profiles:}")
    private Set<UUID> assignedTenantProfiles;

    // EDQS(边缘调度服务)配置
    @Autowired
    private EdqsConfig edqsConfig;

    @Autowired
    private ApplicationContext applicationContext;

    // 当前服务支持的服务类型列表
    private List<ServiceType> serviceTypes;

    // 当前服务的信息对象（包含所有元数据）
    private ServiceInfo serviceInfo;

    @PostConstruct
    public void init() {
        // 生成服务ID（如果未配置）
        if (StringUtils.isEmpty(serviceId)) {
            try {
                serviceId = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                serviceId = StringUtils.randomAlphabetic(10);
            }
        }
        log.info("Current Service ID: {}", serviceId);

        // 解析服务类型
        if (serviceType.equalsIgnoreCase("monolith")) {
            // 单体服务支持所有服务类型
            serviceTypes = List.of(ServiceType.values());
        } else {
            // 微服务模式只支持单一服务类型
            serviceTypes = Collections.singletonList(ServiceType.of(serviceType));
        }
        // 处理租户档案分配
        if (!serviceTypes.contains(ServiceType.TB_RULE_ENGINE) || assignedTenantProfiles == null) {
            // 非规则引擎服务清空分配
            assignedTenantProfiles = Collections.emptySet();
        }
        // 初始化EDQS标签
        if (serviceTypes.contains(ServiceType.EDQS)) {
            if (StringUtils.isBlank(edqsConfig.getLabel())) {
                edqsConfig.setLabel(serviceId);
            }
        }
        // 创建初始服务信息
        generateNewServiceInfoWithCurrentSystemInfo();
    }

    @AfterContextReady
    public void setTransports() {
        // 获取该服务的所有传输服务，并更新当前服务信息
        serviceInfo = ServiceInfo.newBuilder(serviceInfo)
                .addAllTransports(getTransportServices().stream()
                        .map(TbTransportService::getName)
                        .collect(Collectors.toSet()))
                .build();
    }

    private Collection<TbTransportService> getTransportServices() {
        return applicationContext.getBeansOfType(TbTransportService.class).values();
    }

    @Override
    public ServiceInfo getServiceInfo() {
        return serviceInfo;
    }

    /**
     * 判断当前服务实例是否是传入的服务类型
     * <p>
     * 当单体启动的时候，则是规则引擎服务也是核心服务等
     * @param serviceType 服务类型
     * @return true 是属于该服务 false 否
     */
    @Override
    public boolean isService(ServiceType serviceType) {
        return serviceTypes.contains(serviceType);
    }

    /**
     * 生成包含最新系统信息的服务信息
     * @return 更新后的服务信息
     */
    @Override
    public ServiceInfo generateNewServiceInfoWithCurrentSystemInfo() {
        ServiceInfo.Builder builder = ServiceInfo.newBuilder()
                .setServiceId(serviceId)
                .addAllServiceTypes(serviceTypes.stream().map(ServiceType::name).collect(Collectors.toList()))
                .setSystemInfo(getCurrentSystemInfoProto());
        if (CollectionsUtil.isNotEmpty(assignedTenantProfiles)) {
            builder.addAllAssignedTenantProfiles(assignedTenantProfiles.stream().map(UUID::toString).collect(Collectors.toList()));
        }
        builder.setLabel(edqsConfig.getLabel());
        return serviceInfo = builder.build();
    }

    /**
     * 获取当前系统资源信息的Protobuf对象
     * @return 系统信息对象
     */
    private TransportProtos.SystemInfoProto getCurrentSystemInfoProto() {
        TransportProtos.SystemInfoProto.Builder builder = TransportProtos.SystemInfoProto.newBuilder();

        getCpuUsage().ifPresent(builder::setCpuUsage);
        getMemoryUsage().ifPresent(builder::setMemoryUsage);
        getDiscSpaceUsage().ifPresent(builder::setDiskUsage);

        getCpuCount().ifPresent(builder::setCpuCount);
        getTotalMemory().ifPresent(builder::setTotalMemory);
        getTotalDiscSpace().ifPresent(builder::setTotalDiscSpace);

        return builder.build();
    }

}
