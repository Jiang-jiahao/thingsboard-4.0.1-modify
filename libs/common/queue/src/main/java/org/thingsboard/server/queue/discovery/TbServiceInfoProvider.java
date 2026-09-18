package org.thingsboard.server.queue.discovery;

import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.gen.transport.TransportProtos.ServiceInfo;

import java.util.Set;
import java.util.UUID;

/**
 * 服务器实例信息提供器
 */
public interface TbServiceInfoProvider {

    String getServiceId();

    String getServiceType();

    ServiceInfo getServiceInfo();

    boolean isService(ServiceType serviceType);

    ServiceInfo generateNewServiceInfoWithCurrentSystemInfo();

    Set<UUID> getAssignedTenantProfiles();

}
