package com.jnks.iot.server.queue.discovery;

import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.queue.discovery.event.PartitionChangeEvent;

import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Once application is ready or cluster topology changes, this Service will produce {@link PartitionChangeEvent}
 */
public interface PartitionService {

    TopicPartitionInfo resolve(ServiceType serviceType, String queueName, TenantId tenantId, EntityId entityId);

    TopicPartitionInfo resolve(ServiceType serviceType, String queueName, TenantId tenantId, EntityId entityId, Integer partition);

    TopicPartitionInfo resolve(ServiceType serviceType, TenantId tenantId, EntityId entityId);

    List<TopicPartitionInfo> resolveAll(ServiceType serviceType, String queueName, TenantId tenantId, EntityId entityId);

    boolean isMyPartition(ServiceType serviceType, TenantId tenantId, EntityId entityId);

    boolean isSystemPartitionMine(ServiceType serviceType);

    List<Integer> getMyPartitions(QueueKey queueKey);

    String getTopic(QueueKey queueKey);

    /**
     * Received from the Discovery service when network topology is changed.
     * @param currentService - current service information {@link com.jnks.iot.server.gen.transport.TransportProtos.ServiceInfo}
     * @param otherServices - all other discovered services {@link com.jnks.iot.server.gen.transport.TransportProtos.ServiceInfo}
     */
    void recalculatePartitions(TransportProtos.ServiceInfo currentService, List<TransportProtos.ServiceInfo> otherServices);

    /**
     * Get all active service ids by service type
     * @param serviceType to filter the list of services
     * @return list of all active services
     */
    Set<String> getAllServiceIds(ServiceType serviceType);

    Set<TransportProtos.ServiceInfo> getAllServices(ServiceType serviceType);

    Set<TransportProtos.ServiceInfo> getOtherServices(ServiceType serviceType);

    void evictTenantInfo(TenantId tenantId);

    int countTransportsByType(String type);

    void updateQueues(List<TransportProtos.QueueUpdateMsg> queueUpdateMsgs);

    void removeQueues(List<TransportProtos.QueueDeleteMsg> queueDeleteMsgs);

    void removeTenant(TenantId tenantId);

    boolean isManagedByCurrentService(TenantId tenantId);

    int resolvePartitionIndex(UUID entityId, int partitions);

    int resolvePartitionIndex(String key, int partitions);

}
