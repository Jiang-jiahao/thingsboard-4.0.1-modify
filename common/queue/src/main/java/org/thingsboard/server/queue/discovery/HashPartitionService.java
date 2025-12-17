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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import jakarta.annotation.PostConstruct;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.exception.TenantNotFoundException;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.id.TenantProfileId;
import org.thingsboard.server.common.data.util.CollectionsUtil;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.gen.transport.TransportProtos.ServiceInfo;
import org.thingsboard.server.queue.discovery.event.ClusterTopologyChangeEvent;
import org.thingsboard.server.queue.discovery.event.PartitionChangeEvent;
import org.thingsboard.server.queue.discovery.event.ServiceListChangedEvent;
import org.thingsboard.server.queue.util.AfterStartUp;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.thingsboard.server.common.data.DataConstants.CF_QUEUE_NAME;
import static org.thingsboard.server.common.data.DataConstants.CF_STATES_QUEUE_NAME;
import static org.thingsboard.server.common.data.DataConstants.EDGE_QUEUE_NAME;
import static org.thingsboard.server.common.data.DataConstants.MAIN_QUEUE_NAME;

@Service
@Slf4j
public class HashPartitionService implements PartitionService {

    // 系统级核心默认队列负责的topic
    @Value("${queue.core.topic:tb_core}")
    private String coreTopic;

    // 系统级核心默认队列分区数量
    @Value("${queue.core.partitions:10}")
    private Integer corePartitions;

    @Value("${queue.calculated_fields.event_topic:tb_cf_event}")
    private String cfEventTopic;
    @Value("${queue.calculated_fields.state_topic:tb_cf_state}")
    private String cfStateTopic;

    // 系统级版本控制默认队列负责的topic
    @Value("${queue.vc.topic:tb_version_control}")
    private String vcTopic;

    // 系统级版本控制默认队列分区数量
    @Value("${queue.vc.partitions:10}")
    private Integer vcPartitions;

    @Value("${queue.edge.topic:tb_edge}")
    private String edgeTopic;
    @Value("${queue.edge.partitions:10}")
    private Integer edgePartitions;
    @Value("${queue.edqs.partitions:12}")
    private Integer edqsPartitions;
    @Value("${queue.partitions.hash_function_name:murmur3_128}")
    private String hashFunctionName;

    private final ApplicationEventPublisher applicationEventPublisher;

    // 服务信息提供者
    private final TbServiceInfoProvider serviceInfoProvider;

    // 租户路由信息服务
    private final TenantRoutingInfoService tenantRoutingInfoService;

    // 队列路由信息服务
    private final QueueRoutingInfoService queueRoutingInfoService;

    // 主题管理服务
    private final TopicService topicService;

    // 当前服务负责的分区 key：队列key value：分区索引集合
    protected volatile ConcurrentMap<QueueKey, List<Integer>> myPartitions = new ConcurrentHashMap<>();

    // 队列主题映射 key：队列key  value：主题
    private final ConcurrentMap<QueueKey, String> partitionTopicsMap = new ConcurrentHashMap<>();

    // 队列分区数量 key：队列key  value：分区数量
    private final ConcurrentMap<QueueKey, Integer> partitionSizesMap = new ConcurrentHashMap<>();

    // 队列配置
    private final ConcurrentMap<QueueKey, QueueConfig> queueConfigs = new ConcurrentHashMap<>();

    // 租户路由信息缓存
    private final ConcurrentMap<TenantId, TenantRoutingInfo> tenantRoutingInfoMap = new ConcurrentHashMap<>();

    // 当前集群中其他服务节点
    private List<ServiceInfo> currentOtherServices;

    // 按类型分类的传输服务 key：传输类型 value：服务器实例信息
    private final Map<String, List<ServiceInfo>> tbTransportServicesByType = new HashMap<>();

    // 当前服务负责的租户配置
    private volatile Map<TenantProfileId, List<ServiceInfo>> responsibleServices = Collections.emptyMap();

    // 哈希函数实例
    private HashFunction hashFunction;

    public HashPartitionService(TbServiceInfoProvider serviceInfoProvider,
                                TenantRoutingInfoService tenantRoutingInfoService,
                                ApplicationEventPublisher applicationEventPublisher,
                                QueueRoutingInfoService queueRoutingInfoService,
                                TopicService topicService) {
        this.serviceInfoProvider = serviceInfoProvider;
        this.tenantRoutingInfoService = tenantRoutingInfoService;
        this.applicationEventPublisher = applicationEventPublisher;
        this.queueRoutingInfoService = queueRoutingInfoService;
        this.topicService = topicService;
    }

    @PostConstruct
    public void init() {
        // 初始化哈希函数
        this.hashFunction = forName(hashFunctionName);
        // 创建系统级核心默认队列
        QueueKey coreKey = new QueueKey(ServiceType.TB_CORE);
        partitionSizesMap.put(coreKey, corePartitions);
        partitionTopicsMap.put(coreKey, coreTopic);

        // 创建系统级版本控制默认队列
        QueueKey vcKey = new QueueKey(ServiceType.TB_VC_EXECUTOR);
        partitionSizesMap.put(vcKey, vcPartitions);
        partitionTopicsMap.put(vcKey, vcTopic);

        // 如果不是传输服务，则进行规则引擎分区初始化
        if (!isTransport(serviceInfoProvider.getServiceType())) {
            doInitRuleEnginePartitions();
        }

        // 创建系统级边缘计算默认队列
        QueueKey edgeKey = coreKey.withQueueName(EDGE_QUEUE_NAME);
        partitionSizesMap.put(edgeKey, edgePartitions);
        partitionTopicsMap.put(edgeKey, edgeTopic);

        // 创建系统级edqs默认队列
        QueueKey edqsKey = new QueueKey(ServiceType.EDQS);
        partitionSizesMap.put(edqsKey, edqsPartitions);
        partitionTopicsMap.put(edqsKey, "edqs"); // placeholder, not used
    }

    @AfterStartUp(order = AfterStartUp.QUEUE_INFO_INITIALIZATION)
    public void partitionsInit() {
        // 如果是传输服务，等到所有服务都启动后才进行初始化（防止获取不到其他服务路由信息）
        if (isTransport(serviceInfoProvider.getServiceType())) {
            doInitRuleEnginePartitions();
        }
    }

    /**
     * 获取当前服务实例，对应队列负责的分区
     * @param queueKey 队列key
     * @return 对应队列负责的分区索引集合
     */
    @Override
    public List<Integer> getMyPartitions(QueueKey queueKey) {
        return myPartitions.get(queueKey);
    }

    @Override
    public String getTopic(QueueKey queueKey) {
        return partitionTopicsMap.get(queueKey);
    }

    /**
     * 初始化规则引擎分区配置
     * 支持传输服务启动早于核心服务的重试机制
     */
    private void doInitRuleEnginePartitions() {
        // 获取队列路由信息
        List<QueueRoutingInfo> queueRoutingInfoList = getQueueRoutingInfos();
        queueRoutingInfoList.forEach(queue -> {
            QueueKey queueKey = new QueueKey(ServiceType.TB_RULE_ENGINE, queue);
            updateQueue(queueKey, queue.getQueueTopic(), queue.getPartitions());
            queueConfigs.put(queueKey, new QueueConfig(queue));
        });
    }

    /**
     * 获取队列路由信息
     */
    private List<QueueRoutingInfo> getQueueRoutingInfos() {
        List<QueueRoutingInfo> queueRoutingInfoList;
        String serviceType = serviceInfoProvider.getServiceType();
        // 判断是否是传输服务
        if (isTransport(serviceType)) {
            // 可能传输服务启动于tb-core之前，这里尝试获取队列路由信息
            int getQueuesRetries = 10;
            while (true) {
                if (getQueuesRetries > 0) {
                    log.info("Try to get queue routing info.");
                    try {
                        queueRoutingInfoList = queueRoutingInfoService.getAllQueuesRoutingInfo();
                        break;
                    } catch (Exception e) {
                        log.info("Failed to get queues routing info: {}!", e.getMessage());
                        getQueuesRetries--;
                    }
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        log.info("Failed to await queues routing info!", e);
                    }
                } else {
                    throw new RuntimeException("Failed to await queues routing info!");
                }
            }
        } else {
            // 其他服务直接获取队列路由信息
            queueRoutingInfoList = queueRoutingInfoService.getAllQueuesRoutingInfo();
        }
        return queueRoutingInfoList;
    }

    private boolean isTransport(String serviceType) {
        return "tb-transport".equals(serviceType);
    }

    /**
     * 更新队列信息
     * @param queueUpdateMsgs 队列更新消息集合
     */
    @Override
    public void updateQueues(List<TransportProtos.QueueUpdateMsg> queueUpdateMsgs) {
        for (TransportProtos.QueueUpdateMsg queueUpdateMsg : queueUpdateMsgs) {
            // 构造队列路由信息对象
            QueueRoutingInfo queueRoutingInfo = new QueueRoutingInfo(queueUpdateMsg);
            TenantId tenantId = queueRoutingInfo.getTenantId();
            QueueKey queueKey = new QueueKey(ServiceType.TB_RULE_ENGINE, queueRoutingInfo.getQueueName(), tenantId);
            // 更新队列主题和分区数
            updateQueue(queueKey, queueRoutingInfo.getQueueTopic(), queueRoutingInfo.getPartitions());
            // 更新存储队列配置
            queueConfigs.put(queueKey, new QueueConfig(queueRoutingInfo));
            // 如果不是系统租户，则移除租户路由缓存（需要重新获取）
            if (!tenantId.isSysTenantId()) {
                tenantRoutingInfoMap.remove(tenantId);
            }
        }
    }

    /**
     * 移除队列信息
     * @param queueDeleteMsgs 队列移除消息集合
     */
    @Override
    public void removeQueues(List<TransportProtos.QueueDeleteMsg> queueDeleteMsgs) {
        List<QueueKey> queueKeys = queueDeleteMsgs.stream()
                .flatMap(queueDeleteMsg -> {
                    TenantId tenantId = TenantId.fromUUID(new UUID(queueDeleteMsg.getTenantIdMSB(), queueDeleteMsg.getTenantIdLSB()));
                    QueueKey queueKey = new QueueKey(ServiceType.TB_RULE_ENGINE, queueDeleteMsg.getQueueName(), tenantId);
                    // 主队列删除时需要同时删除相关计算字段队列
                    if (queueKey.getQueueName().equals(MAIN_QUEUE_NAME)) {
                        return Stream.of(queueKey, queueKey.withQueueName(CF_QUEUE_NAME),
                                queueKey.withQueueName(CF_STATES_QUEUE_NAME));
                    } else {
                        return Stream.of(queueKey);
                    }
                }).toList();
        // 删除队列和相关缓存
        queueKeys.forEach(queueKey -> {
            removeQueue(queueKey);
            // 移除租户路由缓存
            evictTenantInfo(queueKey.getTenantId());
        });
        // 如果当前服务是规则引擎，则发布分区变更事件
        if (serviceInfoProvider.isService(ServiceType.TB_RULE_ENGINE)) {
            publishPartitionChangeEvent(ServiceType.TB_RULE_ENGINE, queueKeys.stream()
                    .collect(Collectors.toMap(k -> k, k -> Collections.emptySet())), Collections.emptyMap());
        }
    }

    /**
     * 移除指定租户的所有的队列信息
     * @param tenantId 要移除的租户id
     */
    @Override
    public void removeTenant(TenantId tenantId) {
        // 获取租户所有队列Key
        List<QueueKey> queueKeys = partitionSizesMap.keySet().stream()
                .filter(queueKey -> tenantId.equals(queueKey.getTenantId()))
                .flatMap(queueKey -> {
                    // 主队列删除时需要同时删除相关计算字段队列
                    if (queueKey.getQueueName().equals(MAIN_QUEUE_NAME)) {
                        return Stream.of(queueKey, queueKey.withQueueName(CF_QUEUE_NAME),
                                queueKey.withQueueName(CF_STATES_QUEUE_NAME));
                    } else {
                        return Stream.of(queueKey);
                    }
                })
                .toList();
        // 删除队列和相关缓存
        queueKeys.forEach(this::removeQueue);
        evictTenantInfo(tenantId);
    }

    private void updateQueue(QueueKey queueKey, String topic, int partitions) {
        partitionTopicsMap.put(queueKey, topic);
        partitionSizesMap.put(queueKey, partitions);
        // 当更新的是主队列的时候
        if (DataConstants.MAIN_QUEUE_NAME.equals(queueKey.getQueueName())) {
            // 处理计算字段事件队列
            QueueKey cfQueueKey = queueKey.withQueueName(DataConstants.CF_QUEUE_NAME);
            partitionTopicsMap.put(cfQueueKey, cfEventTopic);
            partitionSizesMap.put(cfQueueKey, partitions);
            // 处理计算字段状态队列
            QueueKey cfStatesQueueKey = queueKey.withQueueName(DataConstants.CF_STATES_QUEUE_NAME);
            partitionTopicsMap.put(cfStatesQueueKey, cfStateTopic);
            partitionSizesMap.put(cfStatesQueueKey, partitions);
        }
    }

    private void removeQueue(QueueKey queueKey) {
        myPartitions.remove(queueKey);
        partitionTopicsMap.remove(queueKey);
        partitionSizesMap.remove(queueKey);
        queueConfigs.remove(queueKey);
    }

    @Override
    public boolean isManagedByCurrentService(TenantId tenantId) {
        if (serviceInfoProvider.isService(ServiceType.TB_CORE) || !serviceInfoProvider.isService(ServiceType.TB_RULE_ENGINE)) {
            return true;
        }

        boolean isManaged;
        Set<UUID> assignedTenantProfiles = serviceInfoProvider.getAssignedTenantProfiles();
        // isRegular=false，则表示是专用服务（表明服务启动的配置文件里面配置了对应的租户id），isRegular=true表示常规服务
        boolean isRegular = assignedTenantProfiles.isEmpty();
        // 系统租户始终由常规规则引擎处理
        if (tenantId.isSysTenantId()) {
            // All system queues are always processed on regular rule engines.
            return isRegular;
        }
        // 获取租户路由信息
        TenantRoutingInfo routingInfo = getRoutingInfo(tenantId);
        // 如果配置文件配置了租户配置的分配信息，则按照配置文件进行分配。如果没有则按数据库配置进行分配
        if (isRegular) {
            if (routingInfo.isIsolated()) {
                // 隔离租户：判断是否是当前常规服务负责
                isManaged = hasDedicatedService(routingInfo.getProfileId());
            } else {
                // 非隔离租户：由常规服务管理
                isManaged = true;
            }
        } else {
            if (routingInfo.isIsolated()) {
                // 隔离租户：判断是否是当前专用服务负责
                isManaged = assignedTenantProfiles.contains(routingInfo.getProfileId().getId());
            } else {
                // 非隔离租户：不由专用服务管理
                isManaged = false;
            }
        }
        log.trace("[{}] Tenant {} managed by this service", tenantId, isManaged ? "is" : "is not");
        return isManaged;
    }

    private boolean hasDedicatedService(TenantProfileId profileId) {
        return CollectionsUtil.isEmpty(responsibleServices.get(profileId));
    }

    /**
     * 解析实体id，返回应发送到的queueName队列分区信息（自动计算分区）
     *
     * @param serviceType 服务类型
     * @param queueName 队列名称
     * @param tenantId 租户id
     * @param entityId 实体id
     * @return 分区信息对象
     */
    @Override
    public TopicPartitionInfo resolve(ServiceType serviceType, String queueName, TenantId tenantId, EntityId entityId) {
        // 获取对应的队列键
        QueueKey queueKey = getQueueKey(serviceType, queueName, tenantId);
        return resolve(queueKey, entityId);
    }

    /**
     * 如果指定了分区的索引，则不解析实体id，直接返回应发送到的queueName队列分区信息
     *
     * @param serviceType 服务类型
     * @param queueName 队列名称
     * @param tenantId 租户id
     * @param entityId 实体id
     * @param partition 指定分区索引
     * @return 分区信息对象
     */
    @Override
    public TopicPartitionInfo resolve(ServiceType serviceType, String queueName, TenantId tenantId, EntityId entityId, Integer partition) {
        // 获取对应的队列键
        QueueKey queueKey = getQueueKey(serviceType, queueName, tenantId);
        if (partition != null) {
            // 如果分区索引不为空，则直接构建分区信息对象
            return buildTopicPartitionInfo(queueKey, partition);
        } else {
            // 如果分区索引为空，则解析实体应发送到的分区
            return resolve(queueKey, entityId);
        }
    }

    /**
     * 解析实体id，返回应发送到的主队列分区信息
     * @param serviceType 服务类型
     * @param tenantId 租户id
     * @param entityId 实体id
     * @return 分区信息对象
     */
    @Override
    public TopicPartitionInfo resolve(ServiceType serviceType, TenantId tenantId, EntityId entityId) {
        return resolve(serviceType, null, tenantId, entityId);
    }

    /**
     * 解析实体id，返回应发送到的所有分区信息（用于广播场景）
     * @param serviceType 服务类型
     * @param queueName 队列名称
     * @param tenantId 租户id
     * @param entityId 实体id
     * @return 分区信息对象集合
     */
    @Override
    public List<TopicPartitionInfo> resolveAll(ServiceType serviceType, String queueName, TenantId tenantId, EntityId entityId) {
        // 获取对应的队列键
        QueueKey queueKey = getQueueKey(serviceType, queueName, tenantId);
        // 解析实体id，获取应发送到的queueName队列分区信息
        TopicPartitionInfo tpi = resolve(queueKey, entityId);
        // 检查是否规则引擎服务且分区有效 TODO 不解
        if (serviceType != ServiceType.TB_RULE_ENGINE || tpi.getPartition().isEmpty()) {
            // 表示不是规则引擎服务（非规则引擎服务不需要广播能力），或者分区无效，则直接返回队列信息
            return List.of(tpi);
        }
        // 获取对应的队列配置（实际上对应kafka主题配置）
        QueueConfig queueConfig = queueConfigs.get(queueKey);
        // 检查是否允许消息复制到所有分区
        if (queueConfig != null && queueConfig.isDuplicateMsgToAllPartitions()) {
            int partition = tpi.getPartition().get();
            Integer partitionsCount = partitionSizesMap.get(queueKey);

            List<TopicPartitionInfo> partitions = new ArrayList<>(partitionsCount);
            partitions.add(tpi);
            for (int i = 0; i < partitionsCount; i++) {
                if (i != partition) {
                    partitions.add(buildTopicPartitionInfo(queueKey, i, false));
                }
            }
            return partitions;
        } else {
            return Collections.singletonList(tpi);
        }
    }

    /**
     * 解析实体对应的分区
     * @param queueKey 队列键
     * @param entityId 实体id
     * @return 分区信息对象
     */
    private TopicPartitionInfo resolve(QueueKey queueKey, EntityId entityId) {
        // 获取该队列键的分区数
        Integer partitionSize = partitionSizesMap.get(queueKey);
        if (partitionSize == null) {
            throw new IllegalStateException("Partitions info for queue " + queueKey + " is missing");
        }
        // 采用hash算法，计算分区
        int hash = hash(entityId.getId());
        int partition = Math.abs(hash % partitionSize);

        return buildTopicPartitionInfo(queueKey, partition);
    }

    /**
     * 根据服务类型、队列名称、租户id，来获取队列键
     * <p>
     * 获取对应的队列键，如果普通租户（需指定了隔离租户，不然都统一按系统租户共享队列处理）的指定queueName的队列不存在，则使用相同queueName的系统租户的队列键，
     * 如果系统租户还是没有，则返回系统主队列建
     * <p>
     * 例子：
     * 租户A（没有设置隔离租户），id为1
     * 现在来获取队列名称为queueA的队列键，那么会直接获取系统租户名为queueA的队列键，如果还是没有，则获取系统租户的主队列键
     * <p>
     * 租户B（设置了隔离租户），id为2
     * 现在来获取队列名称为queueB的队列键，那么会先获取对应租户B的队列键，如果获取不到，则获取系统租户名为queueB的队列键，如果还是没有，则获取系统租户的主队列键
     *
     * @param serviceType 服务类型
     * @param queueName 队列名称
     * @param tenantId 租户id
     * @return 队列键对象
     */
    private QueueKey getQueueKey(ServiceType serviceType, String queueName, TenantId tenantId) {
        // 获取隔离的租户id或者是系统租户id（非隔离则表示共享队列，则直接使用系统租户id即可）
        TenantId isolatedOrSystemTenantId = getIsolatedOrSystemTenantId(serviceType, tenantId);
        // 如果没有指定队列名称，则默认使用主队列
        if (queueName == null || queueName.isEmpty()) {
            queueName = MAIN_QUEUE_NAME;
        }
        QueueKey queueKey = new QueueKey(serviceType, queueName, isolatedOrSystemTenantId);
        if (!partitionSizesMap.containsKey(queueKey)) {
            // 队列不存在，则判断是否是系统租户
            if (isolatedOrSystemTenantId.isSysTenantId()) {
                // 如果是系统租户，则表示需要的系统租户对应名称的队列不存在，则返回主队列键
                queueKey = new QueueKey(serviceType, TenantId.SYS_TENANT_ID);
            } else {
                // 如果不是系统租户，则表示普通租户的对应queueName的队列不存在，则获取相同queueName的系统租户的队列键
                queueKey = new QueueKey(serviceType, queueName, TenantId.SYS_TENANT_ID);
                // 如果是不是主队列，并且不存在该队列键（表示系统租户也没有该队列）
                if (!MAIN_QUEUE_NAME.equals(queueName) && !partitionSizesMap.containsKey(queueKey)) {
                    // 直接返回系统租户的主队列键
                    queueKey = new QueueKey(serviceType, TenantId.SYS_TENANT_ID);
                }
                log.warn("Using queue {} instead of isolated {} for tenant {}", queueKey, queueName, isolatedOrSystemTenantId);
            }
        }
        return queueKey;
    }

    /**
     * 检查该实体id对应的分区是否属于当前服务管理
     *
     * @param serviceType 服务类型
     * @param tenantId 租户id
     * @param entityId 实体id
     * @return true 是 false 不是
     */
    @Override
    public boolean isMyPartition(ServiceType serviceType, TenantId tenantId, EntityId entityId) {
        try {
            return resolve(serviceType, tenantId, entityId).isMyPartition();
        } catch (TenantNotFoundException e) {
            log.warn("Tenant with id {} not found", tenantId, new RuntimeException("stacktrace"));
            return false;
        }
    }

    /**
     * 当前服务实例是否负责处理系统队列的分区
     *
     * @param serviceType 服务类型
     * @return true 是 false 否
     */
    @Override
    public boolean isSystemPartitionMine(ServiceType serviceType) {
        return isMyPartition(serviceType, TenantId.SYS_TENANT_ID, TenantId.SYS_TENANT_ID);
    }

    /**
     * 重新计算分区分配（集群拓扑变化时调用）
     * <p>
     * 处理流程：
     * 1. 准备数据结构
     * 2. 添加所有节点（包括当前节点）
     * 3. 计算新分区分配
     * 4. 更新状态并发布变更事件
     * 5. 发布集群拓扑变化事件
     */
    @Override
    public synchronized void recalculatePartitions(ServiceInfo currentService, List<ServiceInfo> otherServices) {
        log.info("Recalculating partitions");
        tbTransportServicesByType.clear();
        logServiceInfo(currentService);
        otherServices.forEach(this::logServiceInfo);
        // 准备数据结构 key：队列键 value 服务器实例集合
        Map<QueueKey, List<ServiceInfo>> queueServicesMap = new HashMap<>();
        // key：租户id value：规则引擎服务器实例集合（也就是说哪些规则引擎服务器实例负责该租户）
        Map<TenantProfileId, List<ServiceInfo>> responsibleServices = new HashMap<>();
        // 添加所有节点
        addNode(currentService, queueServicesMap, responsibleServices);
        for (ServiceInfo other : otherServices) {
            addNode(other, queueServicesMap, responsibleServices);
        }
        queueServicesMap.values().forEach(list -> list.sort(Comparator.comparing(ServiceInfo::getServiceId)));
        responsibleServices.values().forEach(list -> list.sort(Comparator.comparing(ServiceInfo::getServiceId)));

        final ConcurrentMap<QueueKey, List<Integer>> newPartitions = new ConcurrentHashMap<>();
        // 遍历每一个队列的每一个分区，调用 resolveByPartitionIdx(...) 找出“这个分区应该由哪些服务实例负责”
        // 如果当前实例也在其中，就把该分区号记录到 newPartitions 里
        partitionSizesMap.forEach((queueKey, size) -> {
            for (int i = 0; i < size; i++) {
                try {
                    // 获取到该分区应该由哪些服务实例负责（如果指定了assignedTenantProfiles，则按这个来返回实例列表，但是里面还是会在列表中使用hash算法来选择一个实例返回）
                    // 其实这里就是在做每个实例分区的负载均衡
                    List<ServiceInfo> services = resolveByPartitionIdx(queueServicesMap.get(queueKey), queueKey, i, responsibleServices);
                    log.trace("Server responsible for {}[{}] - {}", queueKey, i, services);
                    // 判断当前服务器实例是否存在需要负责，如果存在则设置
                    if (services.contains(currentService)) {
                        newPartitions.computeIfAbsent(queueKey, key -> new ArrayList<>()).add(i);
                    }
                } catch (Exception e) {
                    log.warn("Failed to resolve server responsible for {}[{}]", queueKey, i, e);
                }
            }
        });
        this.responsibleServices = responsibleServices;

        // 更新分区状态
        final ConcurrentMap<QueueKey, List<Integer>> oldPartitions = myPartitions;
        myPartitions = newPartitions;

        // 记录变化后的分区
        Map<QueueKey, Set<TopicPartitionInfo>> changedPartitionsMap = new HashMap<>();
        // 记录变化前的分区（包含了之后被新增的队列，只是value是空值）
        Map<QueueKey, Set<TopicPartitionInfo>> oldPartitionsMap = new HashMap<>();

        // 创建需要被移除的队列集合
        Set<QueueKey> removed = new HashSet<>();
        oldPartitions.forEach((queueKey, partitions) -> {
            if (!newPartitions.containsKey(queueKey)) {
                // 标记旧分区存在但新分区不存在的队列（表示要删除）
                removed.add(queueKey);
            }
        });

        // 规则引擎服务额外检查
        if (serviceInfoProvider.isService(ServiceType.TB_RULE_ENGINE)) {
            partitionSizesMap.keySet().stream()
                    .filter(queueKey -> queueKey.getType() == ServiceType.TB_RULE_ENGINE &&
                            !queueKey.getTenantId().isSysTenantId() &&
                            !newPartitions.containsKey(queueKey))
                    .forEach(removed::add);
        }
        removed.forEach(queueKey -> {
            // 记录移除状态，空集合表示完全移除
            changedPartitionsMap.put(queueKey, Collections.emptySet());
        });

        // 循环新分区，判断队列分区是否发生改变
        myPartitions.forEach((queueKey, partitions) -> {
            // 如果同一个队列，新/旧队列分区数发生改变，或者旧队列不存在该队列，则新增/更新对应的队列分区信息
            if (!partitions.equals(oldPartitions.get(queueKey))) {
                changedPartitionsMap.put(queueKey, toTpiList(queueKey, partitions));
                oldPartitionsMap.put(queueKey, toTpiList(queueKey, oldPartitions.get(queueKey)));
            }
        });

        // 发布分区变更事件，表示该服务器实例负责的分区发生了改变
        if (!changedPartitionsMap.isEmpty()) {
            changedPartitionsMap.entrySet().stream()
                    .collect(Collectors.groupingBy(entry -> entry.getKey().getType(), Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                    .forEach((serviceType, partitionsMap) -> {
                        publishPartitionChangeEvent(serviceType, partitionsMap, oldPartitionsMap);
                    });
        }

        // 处理集群拓扑变化
        if (currentOtherServices == null) {
            currentOtherServices = new ArrayList<>(otherServices);
        } else {
            Set<QueueKey> changes = new HashSet<>();
            Map<QueueKey, List<ServiceInfo>> currentMap = getServiceKeyListMap(currentOtherServices);
            Map<QueueKey, List<ServiceInfo>> newMap = getServiceKeyListMap(otherServices);
            // 更新当前服务实例的其他服务实例列表
            currentOtherServices = otherServices;
            // 循环当前服务列表，找出集群的实例变化
            currentMap.forEach((key, list) -> {
                if (!list.equals(newMap.get(key))) {
                    changes.add(key);
                }
            });
            currentMap.keySet().forEach(newMap::remove);
            changes.addAll(newMap.keySet());

            // 如果队列列表发生了改变，则发布集群拓扑变化事件并打印日志
            if (!changes.isEmpty()) {
                applicationEventPublisher.publishEvent(new ClusterTopologyChangeEvent(this, changes));
                responsibleServices.forEach((profileId, serviceInfos) -> {
                    if (profileId != null) {
                        log.info("Servers responsible for tenant profile {}: {}", profileId, toServiceIds(serviceInfos));
                    } else {
                        log.info("Servers responsible for system queues: {}", toServiceIds(serviceInfos));
                    }
                });
            }
        }
        // 不管是否发生改变，发布服务列表变化事件
        applicationEventPublisher.publishEvent(new ServiceListChangedEvent(otherServices, currentService));
    }

    private void publishPartitionChangeEvent(ServiceType serviceType,
                                             Map<QueueKey, Set<TopicPartitionInfo>> newPartitions,
                                             Map<QueueKey, Set<TopicPartitionInfo>> oldPartitions) {
        log.info("Partitions changed: {}", System.lineSeparator() + newPartitions.entrySet().stream()
                .map(entry -> "[" + entry.getKey() + "] - [" + entry.getValue().stream()
                        .map(tpi -> tpi.getPartition().orElse(-1).toString()).sorted()
                        .collect(Collectors.joining(", ")) + "]")
                .collect(Collectors.joining(System.lineSeparator())));
        PartitionChangeEvent event = new PartitionChangeEvent(this, serviceType, newPartitions, oldPartitions);
        try {
            applicationEventPublisher.publishEvent(event);
        } catch (Exception e) {
            log.error("Failed to publish partition change event {}", event, e);
        }
    }

    private Set<TopicPartitionInfo> toTpiList(QueueKey queueKey, List<Integer> partitions) {
        if (partitions == null) {
            return Collections.emptySet();
        }
        return partitions.stream()
                .map(partition -> buildTopicPartitionInfo(queueKey, partition))
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getAllServiceIds(ServiceType serviceType) {
        return getAllServices(serviceType).stream().map(ServiceInfo::getServiceId).collect(Collectors.toSet());
    }

    @Override
    public Set<ServiceInfo> getAllServices(ServiceType serviceType) {
        Set<ServiceInfo> result = getOtherServices(serviceType);
        ServiceInfo current = serviceInfoProvider.getServiceInfo();
        if (current.getServiceTypesList().contains(serviceType.name())) {
            result.add(current);
        }
        return result;
    }

    @Override
    public Set<ServiceInfo> getOtherServices(ServiceType serviceType) {
        Set<ServiceInfo> result = new HashSet<>();
        if (currentOtherServices != null) {
            for (ServiceInfo serviceInfo : currentOtherServices) {
                if (serviceInfo.getServiceTypesList().contains(serviceType.name())) {
                    result.add(serviceInfo);
                }
            }
        }
        return result;
    }

    /**
     * 解析实体ID对应的分区索引
     * @param entityId
     * @param partitions
     * @return
     */
    @Override
    public int resolvePartitionIndex(UUID entityId, int partitions) {
        return resolvePartitionIndex(hash(entityId), partitions);
    }

    /**
     * 解析字符串键对应的分区索引
     * @param key
     * @param partitions
     * @return
     */
    @Override
    public int resolvePartitionIndex(String key, int partitions) {
        return resolvePartitionIndex(hash(key), partitions);
    }

    /**
     * 根据哈希值和分区总数计算分区索引
     * @param hash
     * @param partitions
     * @return
     */
    private int resolvePartitionIndex(int hash, int partitions) {
        return Math.abs(hash % partitions);
    }

    /**
     * 移除租户路由缓存
     * @param tenantId 租户id
     */
    @Override
    public void evictTenantInfo(TenantId tenantId) {
        tenantRoutingInfoMap.remove(tenantId);
    }

    @Override
    public int countTransportsByType(String type) {
        var list = tbTransportServicesByType.get(type);
        return list == null ? 0 : list.size();
    }

    private Map<QueueKey, List<ServiceInfo>> getServiceKeyListMap(List<ServiceInfo> services) {
        final Map<QueueKey, List<ServiceInfo>> currentMap = new HashMap<>();
        services.forEach(serviceInfo -> {
            for (String serviceTypeStr : serviceInfo.getServiceTypesList()) {
                ServiceType serviceType = ServiceType.of(serviceTypeStr);
                if (ServiceType.TB_RULE_ENGINE.equals(serviceType)) {
                    partitionTopicsMap.keySet().forEach(queueKey ->
                            currentMap.computeIfAbsent(queueKey, key -> new ArrayList<>()).add(serviceInfo));
                } else {
                    QueueKey queueKey = new QueueKey(serviceType);
                    currentMap.computeIfAbsent(queueKey, key -> new ArrayList<>()).add(serviceInfo);
                }
            }
        });
        return currentMap;
    }

    /**
     * 构建TopicPartitionInfo
     * @param queueKey 队列键
     * @param partition 分区索引
     * @return topic分区信息
     */
    private TopicPartitionInfo buildTopicPartitionInfo(QueueKey queueKey, int partition) {
        List<Integer> partitions = myPartitions.get(queueKey);
        // 检查分区是否属于当前服务
        return buildTopicPartitionInfo(queueKey, partition, partitions != null && partitions.contains(partition));
    }

    /**
     * 构建TopicPartitionInfo对象（指定是否属于当前服务）
     * @param queueKey 队列键
     * @param partition 分区索引
     * @param myPartition 分区是否属于当前服务负责
     * @return topic分区信息对象
     */
    private TopicPartitionInfo buildTopicPartitionInfo(QueueKey queueKey, int partition, boolean myPartition) {
        return TopicPartitionInfo.builder()
                .topic(topicService.buildTopicName(partitionTopicsMap.get(queueKey)))
                .partition(partition)
                .tenantId(queueKey.getTenantId())
                .myPartition(myPartition)
                .build();
    }

    /**
     * 检查租户是否为隔离租户（对于一些租户性能要求较高的，可以采用隔离的方式），仅规则引擎服务考虑隔离
     * @param serviceType 服务类型
     * @param tenantId 租户id
     * @return 是否隔离租户
     */
    private boolean isIsolated(ServiceType serviceType, TenantId tenantId) {
        // 系统租户始终非隔离
        if (TenantId.SYS_TENANT_ID.equals(tenantId)) {
            return false;
        }
        // 根据租户id获取租户的信息
        TenantRoutingInfo routingInfo = getRoutingInfo(tenantId);
        if (routingInfo == null) {
            throw new TenantNotFoundException(tenantId);
        }
        // 仅规则引擎服务考虑隔离
        if (serviceType == ServiceType.TB_RULE_ENGINE) {
            return routingInfo.isIsolated();
        }
        return false;
    }

    private TenantRoutingInfo getRoutingInfo(TenantId tenantId) {
        return tenantRoutingInfoMap.computeIfAbsent(tenantId, tenantRoutingInfoService::getRoutingInfo);
    }

    /**
     * 获取实际租户ID（处理隔离逻辑）
     * 如果租户id是隔离的，则返回原租户id；如果租户id不隔离，则返回系统租户id
     * @param serviceType 服务类型
     * @param tenantId 租户id
     * @return 租户id
     */
    protected TenantId getIsolatedOrSystemTenantId(ServiceType serviceType, TenantId tenantId) {
        return isIsolated(serviceType, tenantId) ? tenantId : TenantId.SYS_TENANT_ID;
    }

    private void logServiceInfo(TransportProtos.ServiceInfo server) {
        log.info("[{}] Found common server: {}", server.getServiceId(), server.getServiceTypesList());
    }

    /**
     * 添加服务节点到路由表
     *
     * 处理逻辑：
     * 1. 识别服务类型并添加到对应队列
     * 2. 记录传输服务类型
     * 3. 对于规则引擎，记录租户档案负责关系
     */
    private void addNode(ServiceInfo instance, Map<QueueKey, List<ServiceInfo>> queueServiceList, Map<TenantProfileId, List<ServiceInfo>> responsibleServices) {
        // 循环所有的服务类型（如果是单体启动，则serviceType是所有的类型集合，如果是某一种服务启动，则是其中一个服务类型）
        for (String serviceTypeStr : instance.getServiceTypesList()) {
            ServiceType serviceType = ServiceType.of(serviceTypeStr);
            // 根据不同的服务类型，设置对应的服务节点
            if (ServiceType.TB_RULE_ENGINE.equals(serviceType)) {
                // 判断是否存在规则引擎服务，如果存在则全部添加（因为规则引擎的实例会有多个，这里需要循环）
                partitionTopicsMap.keySet().forEach(key -> {
                    if (key.getType().equals(ServiceType.TB_RULE_ENGINE)) {
                        queueServiceList.computeIfAbsent(key, k -> new ArrayList<>()).add(instance);
                    }
                });
                // 如果配置文件指定了该规则引擎服务器实例的租户分配，则维护租户与规则引擎服务器实例的映射关系
                if (instance.getAssignedTenantProfilesCount() > 0) {
                    for (String profileIdStr : instance.getAssignedTenantProfilesList()) {
                        TenantProfileId profileId;
                        try {
                            profileId = new TenantProfileId(UUID.fromString(profileIdStr));
                        } catch (IllegalArgumentException e) {
                            log.warn("Failed to parse '{}' as tenant profile id", profileIdStr);
                            continue;
                        }
                        responsibleServices.computeIfAbsent(profileId, k -> new ArrayList<>()).add(instance);
                    }
                }
            } else if (ServiceType.TB_CORE.equals(serviceType)) {
                queueServiceList.computeIfAbsent(new QueueKey(serviceType), key -> new ArrayList<>()).add(instance);
                queueServiceList.computeIfAbsent(new QueueKey(serviceType).withQueueName(EDGE_QUEUE_NAME), key -> new ArrayList<>()).add(instance);
            } else if (ServiceType.TB_VC_EXECUTOR.equals(serviceType)) {
                queueServiceList.computeIfAbsent(new QueueKey(serviceType), key -> new ArrayList<>()).add(instance);
            } else if (ServiceType.EDQS.equals(serviceType)) {
                queueServiceList.computeIfAbsent(new QueueKey(serviceType), key -> new ArrayList<>()).add(instance);
            }
        }
        // 获取该实例传输服务开启了哪些，并维护传输服务类型和服务器实例之间的关系
        for (String transportType : instance.getTransportsList()) {
            tbTransportServicesByType.computeIfAbsent(transportType, t -> new ArrayList<>()).add(instance);
        }
    }

    /**
     * 解析指定分区应负责的服务实例
     * @param servers 对应queueKey的服务器实例集合（是集群的，不是单个服务的）
     * @param queueKey 队列key
     * @param partition 指定的分区数量
     * @param responsibleServices 租户id与规则引擎服务器实例的map
     * @return 服务器实例信息集合
     */
    @NotNull
    protected List<ServiceInfo> resolveByPartitionIdx(List<ServiceInfo> servers, QueueKey queueKey, int partition,
                                                      Map<TenantProfileId, List<ServiceInfo>> responsibleServices) {
        if (servers == null || servers.isEmpty()) {
            return Collections.emptyList();
        }
        // 获取租户id
        TenantId tenantId = queueKey.getTenantId();
        if (queueKey.getType() == ServiceType.TB_RULE_ENGINE) {
            if (!responsibleServices.isEmpty()) { // if there are any dedicated servers
                TenantProfileId profileId;
                if (tenantId != null && !tenantId.isSysTenantId()) {
                    TenantRoutingInfo routingInfo = tenantRoutingInfoService.getRoutingInfo(tenantId);
                    profileId = routingInfo.getProfileId();
                } else {
                    profileId = null;
                }
                // 获取到对应负责该租户的服务器实例集合
                List<ServiceInfo> responsible = responsibleServices.get(profileId);
                if (responsible == null) {
                    // 如果此租户没有服务实例负责，则使用没有负责租户的服务器实例集合
                    responsible = servers.stream()
                            .filter(serviceInfo -> serviceInfo.getAssignedTenantProfilesCount() == 0)
                            .sorted(Comparator.comparing(ServiceInfo::getServiceId))
                            .collect(Collectors.toList());
                    if (profileId != null) {
                        log.debug("Using servers {} for profile {}", toServiceIds(responsible), profileId);
                    }
                    responsibleServices.put(profileId, responsible);
                }
                if (responsible.isEmpty()) {
                    return Collections.emptyList();
                }
                servers = responsible;
            }

            int hash = hash(tenantId.getId());
            ServiceInfo server = servers.get(Math.abs((hash + partition) % servers.size()));
            return server != null ? List.of(server) : Collections.emptyList();
        } else if (queueKey.getType() == ServiceType.EDQS) {
            List<List<ServiceInfo>> sets = servers.stream().collect(Collectors.groupingBy(ServiceInfo::getLabel))
                    .entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).toList();
            return sets.get(partition % sets.size());
        } else {
            ServiceInfo server = servers.get(partition % servers.size());
            return server != null ? List.of(server) : Collections.emptyList();
        }
    }

    private int hash(UUID key) {
        return hashFunction.newHasher()
                .putLong(key.getMostSignificantBits())
                .putLong(key.getLeastSignificantBits())
                .hash().asInt();
    }

    private int hash(String key) {
        return hashFunction.newHasher()
                .putString(key, StandardCharsets.UTF_8)
                .hash().asInt();
    }

    public static HashFunction forName(String name) {
        return switch (name) {
            case "murmur3_32" -> Hashing.murmur3_32();
            case "murmur3_128" -> Hashing.murmur3_128();
            case "sha256" -> Hashing.sha256();
            default -> throw new IllegalArgumentException("Can't find hash function with name " + name);
        };
    }

    private List<String> toServiceIds(Collection<ServiceInfo> serviceInfos) {
        return serviceInfos.stream().map(ServiceInfo::getServiceId).collect(Collectors.toList());
    }

    @Data
    public static class QueueConfig {
        // 是否将消息复制到所有分区
        private boolean duplicateMsgToAllPartitions;

        public QueueConfig(QueueRoutingInfo queueRoutingInfo) {
            this.duplicateMsgToAllPartitions = queueRoutingInfo.isDuplicateMsgToAllPartitions();
        }

    }

}
