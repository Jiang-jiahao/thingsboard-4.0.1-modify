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

/**
 * 基于哈希的分区路由服务（{@link PartitionService} 默认实现）。
 * <p>
 * 核心职责：
 * <ul>
 *   <li>维护每条逻辑队列（{@link QueueKey}）的 topic、分区数、队列配置</li>
 *   <li>按「服务类型 + 队列名 + 租户（隔离/共享）」解析应投递的 topic 与 partition</li>
 *   <li>集群拓扑变化时重新计算「当前实例负责哪些分区」，并发布分区变更事件</li>
 * </ul>
 * QueueKey 三要素：type（给哪类服务）+ queueName（哪条命名队列）+ tenantId（system 共享 / 真实租户隔离）。
 */
@Service
@Slf4j
public class HashPartitionService implements PartitionService {

    /** Core 默认队列对应的 Kafka topic */
    @Value("${queue.core.topic:tb_core}")
    private String coreTopic;

    /** Core 默认队列分区数 */
    @Value("${queue.core.partitions:10}")
    private Integer corePartitions;

    /** Calculated Field 事件 topic */
    @Value("${queue.calculated_fields.event_topic:tb_cf_event}")
    private String cfEventTopic;

    /** Calculated Field 状态 topic */
    @Value("${queue.calculated_fields.state_topic:tb_cf_state}")
    private String cfStateTopic;

    /** Version Control 队列 topic */
    @Value("${queue.vc.topic:tb_version_control}")
    private String vcTopic;

    /** Version Control 队列分区数 */
    @Value("${queue.vc.partitions:10}")
    private Integer vcPartitions;

    /** Edge 队列 topic */
    @Value("${queue.edge.topic:tb_edge}")
    private String edgeTopic;

    /** Edge 队列分区数 */
    @Value("${queue.edge.partitions:10}")
    private Integer edgePartitions;

    /** EDQS 队列分区数 */
    @Value("${queue.edqs.partitions:12}")
    private Integer edqsPartitions;

    /** 分区哈希算法名，默认 murmur3_128 */
    @Value("${queue.partitions.hash_function_name:murmur3_128}")
    private String hashFunctionName;

    private final ApplicationEventPublisher applicationEventPublisher;

    /** 当前进程的服务身份（serviceId、serviceTypes、assignedTenantProfiles 等） */
    private final TbServiceInfoProvider serviceInfoProvider;

    /** 查询租户是否隔离、所属 TenantProfile 等路由信息 */
    private final TenantRoutingInfoService tenantRoutingInfoService;

    /** 查询规则引擎队列定义（名称、topic、分区数、是否广播到全部分区等） */
    private final QueueRoutingInfoService queueRoutingInfoService;

    /** Topic 命名（加前缀等） */
    private final TopicService topicService;

    /**
     * 当前实例负责消费的分区：QueueKey → 分区索引列表。
     * 拓扑变化后由 {@link #recalculatePartitions} 整体替换。
     */
    protected volatile ConcurrentMap<QueueKey, List<Integer>> myPartitions = new ConcurrentHashMap<>();

    /** QueueKey → 逻辑 topic 名（尚未加集群前缀） */
    private final ConcurrentMap<QueueKey, String> partitionTopicsMap = new ConcurrentHashMap<>();

    /** QueueKey → 分区总数 */
    private final ConcurrentMap<QueueKey, Integer> partitionSizesMap = new ConcurrentHashMap<>();

    /** QueueKey → 队列级配置（如是否 duplicateMsgToAllPartitions） */
    private final ConcurrentMap<QueueKey, QueueConfig> queueConfigs = new ConcurrentHashMap<>();

    /** 租户路由信息本地缓存 */
    private final ConcurrentMap<TenantId, TenantRoutingInfo> tenantRoutingInfoMap = new ConcurrentHashMap<>();

    /** 上一次 recalculate 时已知的「其他服务实例」列表，用于判断集群拓扑是否变化 */
    private List<ServiceInfo> currentOtherServices;

    /** 传输类型 → 开启了该传输的服务实例列表（如 mqtt、coap） */
    private final Map<String, List<ServiceInfo>> tbTransportServicesByType = new HashMap<>();

    /**
     * TenantProfileId → 「专管该 租户Profile 的 Rule Engine 实例」列表。
     * 无专管实例时，隔离租户可能回落到「未绑定任何 Profile 的常规 RE」。
     */
    private volatile Map<TenantProfileId, List<ServiceInfo>> responsibleServices = Collections.emptyMap();

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

    /**
     * 启动时注册系统级队列元数据（Core / VC / Edge / EDQS）。
     * Rule Engine 队列：非 Transport 进程在此初始化；Transport 延后到 {@link #partitionsInit}。
     */
    @PostConstruct
    public void init() {
        this.hashFunction = forName(hashFunctionName);

        // QK(Main, TB_CORE, system)
        QueueKey coreKey = new QueueKey(ServiceType.TB_CORE);
        partitionSizesMap.put(coreKey, corePartitions);
        partitionTopicsMap.put(coreKey, coreTopic);

        // QK(Main, TB_VC_EXECUTOR, system)
        QueueKey vcKey = new QueueKey(ServiceType.TB_VC_EXECUTOR);
        partitionSizesMap.put(vcKey, vcPartitions);
        partitionTopicsMap.put(vcKey, vcTopic);

        // Transport 可能早于 Core 启动，此时尚拉不到 RE 队列定义，故跳过（因为只有规则引擎的队列是通过数据库配置的，所以这里进行读取）
        if (!isTransport(serviceInfoProvider.getServiceType())) {
            doInitRuleEnginePartitions();
        }

        // Edge 挂在 Core 服务类型下，但 queueName 为 Edge
        QueueKey edgeKey = coreKey.withQueueName(EDGE_QUEUE_NAME);
        partitionSizesMap.put(edgeKey, edgePartitions);
        partitionTopicsMap.put(edgeKey, edgeTopic);

        QueueKey edqsKey = new QueueKey(ServiceType.EDQS);
        partitionSizesMap.put(edqsKey, edqsPartitions);
        partitionTopicsMap.put(edqsKey, "edqs"); // placeholder, not used
    }

    /**
     * 应用就绪后：Transport 再初始化 Rule Engine 队列元数据，避免启动过早拉不到路由信息。
     */
    @AfterStartUp(order = AfterStartUp.QUEUE_INFO_INITIALIZATION)
    public void partitionsInit() {
        if (isTransport(serviceInfoProvider.getServiceType())) {
            doInitRuleEnginePartitions();
        }
    }

    /** 当前实例在指定 QueueKey 上负责的分区索引；未负责则返回 null */
    @Override
    public List<Integer> getMyPartitions(QueueKey queueKey) {
        return myPartitions.get(queueKey);
    }

    /** 指定 QueueKey 对应的逻辑 topic 名 */
    @Override
    public String getTopic(QueueKey queueKey) {
        return partitionTopicsMap.get(queueKey);
    }

    /** 从 QueueRoutingInfo 加载全部 Rule Engine 队列，写入 topic / 分区数 / QueueConfig */
    private void doInitRuleEnginePartitions() {
        List<QueueRoutingInfo> queueRoutingInfoList = getQueueRoutingInfos();
        queueRoutingInfoList.forEach(queue -> {
            QueueKey queueKey = new QueueKey(ServiceType.TB_RULE_ENGINE, queue);
            updateQueue(queueKey, queue.getQueueTopic(), queue.getPartitions());
            queueConfigs.put(queueKey, new QueueConfig(queue));
        });
    }

    /**
     * 拉取全部队列路由定义。
     * Transport 侧带重试：可能比 tb-core 先启动，一时拿不到队列信息。
     */
    private List<QueueRoutingInfo> getQueueRoutingInfos() {
        List<QueueRoutingInfo> queueRoutingInfoList;
        String serviceType = serviceInfoProvider.getServiceType();
        if (isTransport(serviceType)) {
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
            queueRoutingInfoList = queueRoutingInfoService.getAllQueuesRoutingInfo();
        }
        return queueRoutingInfoList;
    }

    private boolean isTransport(String serviceType) {
        return "tb-transport".equals(serviceType);
    }

    /**
     * 接收队列创建/更新通知：刷新对应 QueueKey 的 topic、分区数、配置；（只有规则引擎的队列会发生修改）
     * 非系统租户会清掉租户路由缓存，下次按新配置重新加载。
     */
    @Override
    public void updateQueues(List<TransportProtos.QueueUpdateMsg> queueUpdateMsgs) {
        for (TransportProtos.QueueUpdateMsg queueUpdateMsg : queueUpdateMsgs) {
            QueueRoutingInfo queueRoutingInfo = new QueueRoutingInfo(queueUpdateMsg);
            TenantId tenantId = queueRoutingInfo.getTenantId();
            QueueKey queueKey = new QueueKey(ServiceType.TB_RULE_ENGINE, queueRoutingInfo.getQueueName(), tenantId);
            updateQueue(queueKey, queueRoutingInfo.getQueueTopic(), queueRoutingInfo.getPartitions());
            queueConfigs.put(queueKey, new QueueConfig(queueRoutingInfo));
            if (!tenantId.isSysTenantId()) {
                tenantRoutingInfoMap.remove(tenantId);
            }
        }
    }

    /**
     * 接收队列删除通知：移除 QueueKey 相关元数据；
     * 删除 Main 时一并删除关联的 CF / CF_STATES 逻辑队列；
     * 若本机是 Rule Engine，再发布「这些队列分区已空」的变更事件。
     */
    @Override
    public void removeQueues(List<TransportProtos.QueueDeleteMsg> queueDeleteMsgs) {
        List<QueueKey> queueKeys = queueDeleteMsgs.stream()
                .flatMap(queueDeleteMsg -> {
                    TenantId tenantId = TenantId.fromUUID(new UUID(queueDeleteMsg.getTenantIdMSB(), queueDeleteMsg.getTenantIdLSB()));
                    QueueKey queueKey = new QueueKey(ServiceType.TB_RULE_ENGINE, queueDeleteMsg.getQueueName(), tenantId);
                    // Main 与 Calculated Field 队列绑定，删 Main 时连带清理
                    if (queueKey.getQueueName().equals(MAIN_QUEUE_NAME)) {
                        return Stream.of(queueKey, queueKey.withQueueName(CF_QUEUE_NAME),
                                queueKey.withQueueName(CF_STATES_QUEUE_NAME));
                    } else {
                        return Stream.of(queueKey);
                    }
                }).toList();
        queueKeys.forEach(queueKey -> {
            removeQueue(queueKey);
            evictTenantInfo(queueKey.getTenantId());
        });
        if (serviceInfoProvider.isService(ServiceType.TB_RULE_ENGINE)) {
            publishPartitionChangeEvent(ServiceType.TB_RULE_ENGINE, queueKeys.stream()
                    .collect(Collectors.toMap(k -> k, k -> Collections.emptySet())), Collections.emptyMap());
        }
    }

    /** 租户删除：清掉该租户名下所有 QueueKey（含 Main 关联的 CF 队列）及路由缓存 */
    @Override
    public void removeTenant(TenantId tenantId) {
        List<QueueKey> queueKeys = partitionSizesMap.keySet().stream()
                .filter(queueKey -> tenantId.equals(queueKey.getTenantId()))
                .flatMap(queueKey -> {
                    if (queueKey.getQueueName().equals(MAIN_QUEUE_NAME)) {
                        return Stream.of(queueKey, queueKey.withQueueName(CF_QUEUE_NAME),
                                queueKey.withQueueName(CF_STATES_QUEUE_NAME));
                    } else {
                        return Stream.of(queueKey);
                    }
                })
                .toList();
        queueKeys.forEach(this::removeQueue);
        evictTenantInfo(tenantId);
    }

    /**
     * 写入/更新一条逻辑队列的 topic 与分区数。
     * 若 queueName 为 Main，自动挂上同 tenant 下的 CF 事件队列与 CF 状态队列（分区数与 Main 相同）。
     */
    private void updateQueue(QueueKey queueKey, String topic, int partitions) {
        partitionTopicsMap.put(queueKey, topic);
        partitionSizesMap.put(queueKey, partitions);
        if (DataConstants.MAIN_QUEUE_NAME.equals(queueKey.getQueueName())) {
            QueueKey cfQueueKey = queueKey.withQueueName(DataConstants.CF_QUEUE_NAME);
            partitionTopicsMap.put(cfQueueKey, cfEventTopic);
            partitionSizesMap.put(cfQueueKey, partitions);

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

    /**
     * 判断「当前进程」是否应处理该租户的规则引擎流量。
     * <p>
     * 规则简述：
     * <ul>
     *   <li>本机是 Core，或根本不是 Rule Engine → 直接 true（本方法主要约束 RE）</li>
     *   <li>常规 RE（未配置 assignedTenantProfiles）：处理系统队列、非隔离租户；
     *       隔离租户仅当集群里没有专管该 Profile 的专用 RE 时，才由常规 RE 兜底</li>
     *   <li>专用 RE（配置了 assignedTenantProfiles）：只处理绑定到这些 Profile 的隔离租户</li>
     * </ul>
     */
    @Override
    public boolean isManagedByCurrentService(TenantId tenantId) {
        // serviceInfoProvider.isService(ServiceType.TB_CORE)条件不能去除，因为存在单体启动的时候直接处理所有的RE流量
        if (serviceInfoProvider.isService(ServiceType.TB_CORE) || !serviceInfoProvider.isService(ServiceType.TB_RULE_ENGINE)) {
            return true;
        }

        boolean isManaged;
        Set<UUID> assignedTenantProfiles = serviceInfoProvider.getAssignedTenantProfiles();
        // assignedTenantProfiles 为空 = 常规 RE；非空 = 专用 RE（只服务指定 TenantProfile）
        boolean isRegular = assignedTenantProfiles.isEmpty();
        if (tenantId.isSysTenantId()) {
            // 为空 = 常规 RE，则表示该实例能处理系统租户RE流量；非空 = 专用 RE，则表示该实例不能处理系统租户RE流量，只处理专属tenant
            return isRegular;
        }
        TenantRoutingInfo routingInfo = getRoutingInfo(tenantId);
        if (isRegular) {
            // 该实例是常规RE
            if (routingInfo.isIsolated()) {
                // 隔离租户：仅当没有专用 RE 认领该 Profile 时，才由常规 RE 管理
                isManaged = hasDedicatedService(routingInfo.getProfileId());
            } else {
                isManaged = true;
            }
        } else {
            // 该实例是专用RE
            if (routingInfo.isIsolated()) {
                // 判断是否由该实例管理
                isManaged = assignedTenantProfiles.contains(routingInfo.getProfileId().getId());
            } else {
                // 专用 RE 不接非隔离租户
                isManaged = false;
            }
        }
        log.trace("[{}] Tenant {} managed by this service", tenantId, isManaged ? "is" : "is not");
        return isManaged;
    }

    /** responsibleServices 中该 Profile 尚无专管实例列表（或为空）→ 视为没有专用 RE */
    private boolean hasDedicatedService(TenantProfileId profileId) {
        return CollectionsUtil.isEmpty(responsibleServices.get(profileId));
    }

    /**
     * 解析消息应发往的 topic + partition（按 entityId 哈希选分区）。
     * 先经 {@link #getQueueKey} 选定逻辑队列，再算分区。
     */
    @Override
    public TopicPartitionInfo resolve(ServiceType serviceType, String queueName, TenantId tenantId, EntityId entityId) {
        QueueKey queueKey = getQueueKey(serviceType, queueName, tenantId);
        return resolve(queueKey, entityId);
    }

    /**
     * 同上，但可显式指定 partition；partition 为 null 时仍按 entityId 哈希。
     */
    @Override
    public TopicPartitionInfo resolve(ServiceType serviceType, String queueName, TenantId tenantId, EntityId entityId, Integer partition) {
        QueueKey queueKey = getQueueKey(serviceType, queueName, tenantId);
        if (partition != null) {
            return buildTopicPartitionInfo(queueKey, partition);
        } else {
            return resolve(queueKey, entityId);
        }
    }

    /** 使用默认队列名 Main 做 resolve */
    @Override
    public TopicPartitionInfo resolve(ServiceType serviceType, TenantId tenantId, EntityId entityId) {
        return resolve(serviceType, null, tenantId, entityId);
    }

    /**
     * 解析投递目标；若该 Rule Engine 队列开启了「复制到全部分区」，则返回全部 TopicPartitionInfo，
     * 主分区 isMyPartition 按本机负责情况标记，其余副本分区标记为非本机。
     * 非 RE 或无法得到有效分区时，只返回单个 TPI。
     */
    @Override
    public List<TopicPartitionInfo> resolveAll(ServiceType serviceType, String queueName, TenantId tenantId, EntityId entityId) {
        QueueKey queueKey = getQueueKey(serviceType, queueName, tenantId);
        TopicPartitionInfo tpi = resolve(queueKey, entityId);
        // 非规则引擎，或不需要分区维度时，不做广播展开
        if (serviceType != ServiceType.TB_RULE_ENGINE || tpi.getPartition().isEmpty()) {
            return List.of(tpi);
        }
        QueueConfig queueConfig = queueConfigs.get(queueKey);
        if (queueConfig != null && queueConfig.isDuplicateMsgToAllPartitions()) {
            int partition = tpi.getPartition().get();
            Integer partitionsCount = partitionSizesMap.get(queueKey);

            List<TopicPartitionInfo> partitions = new ArrayList<>(partitionsCount);
            partitions.add(tpi);
            for (int i = 0; i < partitionsCount; i++) {
                if (i != partition) {
                    // 副本分区：myPartition=false，表示投递用，不代表本机负责消费
                    partitions.add(buildTopicPartitionInfo(queueKey, i, false));
                }
            }
            return partitions;
        } else {
            return Collections.singletonList(tpi);
        }
    }

    /** entityId 哈希取模得到分区，再组装 TopicPartitionInfo */
    private TopicPartitionInfo resolve(QueueKey queueKey, EntityId entityId) {
        Integer partitionSize = partitionSizesMap.get(queueKey);
        if (partitionSize == null) {
            throw new IllegalStateException("Partitions info for queue " + queueKey + " is missing");
        }
        int hash = hash(entityId.getId());
        int partition = Math.abs(hash % partitionSize);

        return buildTopicPartitionInfo(queueKey, partition);
    }

    /**
     * 将 (serviceType, queueName, 业务 tenantId) 收敛为实际存在的 {@link QueueKey}。
     * <p>
     * 步骤：
     * <ol>
     *   <li>隔离租户且目标为 RE → QueueKey.tenantId = 真实租户；否则改为 system（共享队列）</li>
     *   <li>queueName 空则用 Main</li>
     *   <li>若该 QueueKey 尚未注册到 partitionSizesMap，则回退：
     *       system 侧缺队列 → system Main；
     *       隔离租户缺专用队列 → 先试 system 同名队列，再不行 → system Main</li>
     * </ol>
     * 注意：业务消息上的 tenantId 与 QueueKey.tenantId 不一定相同（非隔离租户共用 system）。
     */
    private QueueKey getQueueKey(ServiceType serviceType, String queueName, TenantId tenantId) {
        TenantId isolatedOrSystemTenantId = getIsolatedOrSystemTenantId(serviceType, tenantId);
        if (queueName == null || queueName.isEmpty()) {
            queueName = MAIN_QUEUE_NAME;
        }
        QueueKey queueKey = new QueueKey(serviceType, queueName, isolatedOrSystemTenantId);
        if (!partitionSizesMap.containsKey(queueKey)) {
            if (isolatedOrSystemTenantId.isSysTenantId()) {
                // 共享队列里找不到指定名 → 退回 Main
                queueKey = new QueueKey(serviceType, TenantId.SYS_TENANT_ID);
            } else {
                // 隔离租户专用队列不存在 → 尝试系统同名队列（隔离租户「应该」有专用队列；这里防的是「应该有但当前没有」——用系统同名/Main 顶一下，保证还能路由，同时打日志提醒配置有缺口）
                queueKey = new QueueKey(serviceType, queueName, TenantId.SYS_TENANT_ID);
                if (!MAIN_QUEUE_NAME.equals(queueName) && !partitionSizesMap.containsKey(queueKey)) {
                    // 如果同名队列也不存在，则用系统队列顶一下
                    queueKey = new QueueKey(serviceType, TenantId.SYS_TENANT_ID);
                }
                log.warn("Using queue {} instead of isolated {} for tenant {}", queueKey, queueName, isolatedOrSystemTenantId);
            }
        }
        return queueKey;
    }

    /** 该实体在默认 Main 队列上算出的分区是否由本机负责 */
    @Override
    public boolean isMyPartition(ServiceType serviceType, TenantId tenantId, EntityId entityId) {
        try {
            return resolve(serviceType, tenantId, entityId).isMyPartition();
        } catch (TenantNotFoundException e) {
            log.warn("Tenant with id {} not found", tenantId, new RuntimeException("stacktrace"));
            return false;
        }
    }

    /** 系统租户在指定服务类型下的分区是否由本机负责（常用于判断是否处理系统级任务） */
    @Override
    public boolean isSystemPartitionMine(ServiceType serviceType) {
        return isMyPartition(serviceType, TenantId.SYS_TENANT_ID, TenantId.SYS_TENANT_ID);
    }

    /**
     * 集群拓扑变化时重算分区归属（由 Discovery 在节点上下线时调用）。
     * <p>
     * 流程：汇总各 QueueKey 上的候选服务列表 → 对每个分区调用 {@link #resolveByPartitionIdx}
     * 选出负责实例 → 更新 {@link #myPartitions} → 有变化则发 {@link PartitionChangeEvent} →
     * 其他节点集合有变则发 {@link ClusterTopologyChangeEvent} → 最后发 {@link ServiceListChangedEvent}。
     */
    @Override
    public synchronized void recalculatePartitions(ServiceInfo currentService, List<ServiceInfo> otherServices) {
        log.info("Recalculating partitions");
        tbTransportServicesByType.clear();
        logServiceInfo(currentService);
        otherServices.forEach(this::logServiceInfo);

        // QueueKey → 可参与该队列分区竞选的服务实例
        Map<QueueKey, List<ServiceInfo>> queueServicesMap = new HashMap<>();
        // TenantProfileId → 专管该 Profile 的 RE 服务实例
        Map<TenantProfileId, List<ServiceInfo>> responsibleServices = new HashMap<>();

        addNode(currentService, queueServicesMap, responsibleServices);
        for (ServiceInfo other : otherServices) {
            addNode(other, queueServicesMap, responsibleServices);
        }
        queueServicesMap.values().forEach(list -> list.sort(Comparator.comparing(ServiceInfo::getServiceId)));
        responsibleServices.values().forEach(list -> list.sort(Comparator.comparing(ServiceInfo::getServiceId)));

        final ConcurrentMap<QueueKey, List<Integer>> newPartitions = new ConcurrentHashMap<>();
        // 对每条逻辑队列的每个分区，算出负责实例；若包含本机则记入 newPartitions
        partitionSizesMap.forEach((queueKey, size) -> {
            for (int i = 0; i < size; i++) {
                try {
                    List<ServiceInfo> services = resolveByPartitionIdx(queueServicesMap.get(queueKey), queueKey, i, responsibleServices);
                    log.trace("Server responsible for {}[{}] - {}", queueKey, i, services);
                    if (services.contains(currentService)) {
                        newPartitions.computeIfAbsent(queueKey, key -> new ArrayList<>()).add(i);
                    }
                } catch (Exception e) {
                    log.warn("Failed to resolve server responsible for {}[{}]", queueKey, i, e);
                }
            }
        });
        this.responsibleServices = responsibleServices;

        final ConcurrentMap<QueueKey, List<Integer>> oldPartitions = myPartitions;
        myPartitions = newPartitions;

        Map<QueueKey, Set<TopicPartitionInfo>> changedPartitionsMap = new HashMap<>();
        Map<QueueKey, Set<TopicPartitionInfo>> oldPartitionsMap = new HashMap<>();

        // 旧有、新无 → 视为本机不再负责该 QueueKey（分区集合置空）
        Set<QueueKey> removed = new HashSet<>();
        oldPartitions.forEach((queueKey, partitions) -> {
            if (!newPartitions.containsKey(queueKey)) {
                removed.add(queueKey);
            }
        });

        // Rule Engine：隔离租户队列若本轮未分到任何分区，也视为移除，促使消费者退订
        if (serviceInfoProvider.isService(ServiceType.TB_RULE_ENGINE)) {
            partitionSizesMap.keySet().stream()
                    .filter(queueKey -> queueKey.getType() == ServiceType.TB_RULE_ENGINE &&
                            !queueKey.getTenantId().isSysTenantId() &&
                            !newPartitions.containsKey(queueKey))
                    .forEach(removed::add);
        }
        removed.forEach(queueKey -> {
            changedPartitionsMap.put(queueKey, Collections.emptySet());
        });

        // 分区列表相对旧值有变化 → 记入 changed / old 两份 map，供事件对比（用于构造发布分区改变事件）
        myPartitions.forEach((queueKey, partitions) -> {
            if (!partitions.equals(oldPartitions.get(queueKey))) {
                changedPartitionsMap.put(queueKey, toTpiList(queueKey, partitions));
                oldPartitionsMap.put(queueKey, toTpiList(queueKey, oldPartitions.get(queueKey)));
            }
        });

        if (!changedPartitionsMap.isEmpty()) {
            changedPartitionsMap.entrySet().stream()
                    .collect(Collectors.groupingBy(entry -> entry.getKey().getType(), Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                    .forEach((serviceType, partitionsMap) -> {
                        // partitionsMap：本 ServiceType 下发生变化的 QueueKey → 新分区集合（含「本机不再负责」的空集合，以及分区列表有增减的）
                        // oldPartitionsMap：仅分区列表发生变化的 QueueKey → 变更前的旧分区集合（整队列被移除的不在此 map 中，只在 partitionsMap 里以 emptySet 表示）
                        publishPartitionChangeEvent(serviceType, partitionsMap, oldPartitionsMap);
                    });
        }

        // 其他节点集合相对上次是否变化 → 集群拓扑事件
        if (currentOtherServices == null) {
            currentOtherServices = new ArrayList<>(otherServices);
        } else {
            Set<QueueKey> changes = new HashSet<>();
            Map<QueueKey, List<ServiceInfo>> currentMap = getServiceKeyListMap(currentOtherServices);
            Map<QueueKey, List<ServiceInfo>> newMap = getServiceKeyListMap(otherServices);
            currentOtherServices = otherServices;
            currentMap.forEach((key, list) -> {
                if (!list.equals(newMap.get(key))) {
                    changes.add(key);
                }
            });
            currentMap.keySet().forEach(newMap::remove);
            changes.addAll(newMap.keySet());

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
        // 主要用于通知transport服务，用于对应设备的负载均衡
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

    /** 当前集群中声明了指定 ServiceType 的全部实例（含本机） */
    @Override
    public Set<ServiceInfo> getAllServices(ServiceType serviceType) {
        Set<ServiceInfo> result = getOtherServices(serviceType);
        ServiceInfo current = serviceInfoProvider.getServiceInfo();
        if (current.getServiceTypesList().contains(serviceType.name())) {
            result.add(current);
        }
        return result;
    }

    /** 其他节点中声明了指定 ServiceType 的实例（不含本机） */
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

    /** 按实体 UUID 哈希计算分区索引 */
    @Override
    public int resolvePartitionIndex(UUID entityId, int partitions) {
        return resolvePartitionIndex(hash(entityId), partitions);
    }

    /** 按字符串键哈希计算分区索引 */
    @Override
    public int resolvePartitionIndex(String key, int partitions) {
        return resolvePartitionIndex(hash(key), partitions);
    }

    private int resolvePartitionIndex(int hash, int partitions) {
        return Math.abs(hash % partitions);
    }

    @Override
    public void evictTenantInfo(TenantId tenantId) {
        tenantRoutingInfoMap.remove(tenantId);
    }

    /** 集群中声明了某传输类型的实例数量 */
    @Override
    public int countTransportsByType(String type) {
        var list = tbTransportServicesByType.get(type);
        return list == null ? 0 : list.size();
    }

    /**
     * 把「其他服务列表」按 QueueKey 归组，用于对比拓扑变化。
     * Rule Engine：挂到所有已注册的 RE QueueKey 下；其他类型：挂到对应默认 QueueKey。
     */
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

    /** 组装 TPI，并根据 myPartitions 判断该分区是否本机负责 */
    private TopicPartitionInfo buildTopicPartitionInfo(QueueKey queueKey, int partition) {
        List<Integer> partitions = myPartitions.get(queueKey);
        return buildTopicPartitionInfo(queueKey, partition, partitions != null && partitions.contains(partition));
    }

    private TopicPartitionInfo buildTopicPartitionInfo(QueueKey queueKey, int partition, boolean myPartition) {
        return TopicPartitionInfo.builder()
                .topic(topicService.buildTopicName(partitionTopicsMap.get(queueKey)))
                .partition(partition)
                .tenantId(queueKey.getTenantId())
                .myPartition(myPartition)
                .build();
    }

    /**
     * 是否走「租户隔离队列」。
     * 仅 TB_RULE_ENGINE 关注隔离；系统租户永远非隔离；其他服务类型一律按共享处理。
     */
    private boolean isIsolated(ServiceType serviceType, TenantId tenantId) {
        if (TenantId.SYS_TENANT_ID.equals(tenantId)) {
            return false;
        }
        TenantRoutingInfo routingInfo = getRoutingInfo(tenantId);
        if (routingInfo == null) {
            throw new TenantNotFoundException(tenantId);
        }
        if (serviceType == ServiceType.TB_RULE_ENGINE) {
            return routingInfo.isIsolated();
        }
        return false;
    }

    private TenantRoutingInfo getRoutingInfo(TenantId tenantId) {
        return tenantRoutingInfoMap.computeIfAbsent(tenantId, tenantRoutingInfoService::getRoutingInfo);
    }

    /**
     * 写入 QueueKey 时使用的 tenantId：
     * 隔离 RE 租户 → 真实 tenantId；否则 → SYS_TENANT_ID（改为SYS_TENANT_ID，实际上就是共享队列，为了QueueKey的共享）。
     */
    protected TenantId getIsolatedOrSystemTenantId(ServiceType serviceType, TenantId tenantId) {
        return isIsolated(serviceType, tenantId) ? tenantId : TenantId.SYS_TENANT_ID;
    }

    private void logServiceInfo(TransportProtos.ServiceInfo server) {
        log.info("[{}] Found common server: {}", server.getServiceId(), server.getServiceTypesList());
    }

    /**
     * 将一个服务实例登记进「QueueKey → 候选实例」与「Profile → 专用 RE」两张表，
     * 并记录其开启的传输类型。
     */
    private void addNode(ServiceInfo instance, Map<QueueKey, List<ServiceInfo>> queueServiceList, Map<TenantProfileId, List<ServiceInfo>> responsibleServices) {
        // 单体进程可能同时带多种 serviceType
        for (String serviceTypeStr : instance.getServiceTypesList()) {
            ServiceType serviceType = ServiceType.of(serviceTypeStr);
            if (ServiceType.TB_RULE_ENGINE.equals(serviceType)) {
                // 每个 RE 实例都作为所有已注册 RE QueueKey 的候选消费者
                partitionTopicsMap.keySet().forEach(key -> {
                    if (key.getType().equals(ServiceType.TB_RULE_ENGINE)) {
                        queueServiceList.computeIfAbsent(key, k -> new ArrayList<>()).add(instance);
                    }
                });
                // 配置了 assignedTenantProfiles 的实例记入专用 RE 表
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
        for (String transportType : instance.getTransportsList()) {
            tbTransportServicesByType.computeIfAbsent(transportType, t -> new ArrayList<>()).add(instance);
        }
    }

    /**
     * 给定 QueueKey 与分区下标，从候选实例中选出负责者。
     * <ul>
     *   <li>RE：若存在专用 RE，先按 QueueKey.tenantId 对应 Profile 缩小候选集；
     *       再按 hash(tenantId)+partition 在候选集中取模，得到单一实例</li>
     *   <li>EDQS：按 ServiceInfo.label 分组后，用 partition % 组数选一组</li>
     *   <li>其他：partition % 实例数 选单一实例</li>
     * </ul>
     *
     * @param servers             该 QueueKey 上的候选服务列表（可能为 null）
     * @param partition           分区下标（不是「分区总数」）
     * @param responsibleServices Profile → 专用 RE；方法内可能回填「常规 RE」兜底列表
     */
    @NotNull
    protected List<ServiceInfo> resolveByPartitionIdx(List<ServiceInfo> servers, QueueKey queueKey, int partition,
                                                      Map<TenantProfileId, List<ServiceInfo>> responsibleServices) {
        if (servers == null || servers.isEmpty()) {
            return Collections.emptyList();
        }
        TenantId tenantId = queueKey.getTenantId();
        if (queueKey.getType() == ServiceType.TB_RULE_ENGINE) {
            if (!responsibleServices.isEmpty()) { // 集群里存在任意专用 RE 时，进入 Profile 分流逻辑
                TenantProfileId profileId;
                if (tenantId != null && !tenantId.isSysTenantId()) {
                    TenantRoutingInfo routingInfo = tenantRoutingInfoService.getRoutingInfo(tenantId);
                    profileId = routingInfo.getProfileId();
                } else {
                    profileId = null; // 系统队列
                }
                List<ServiceInfo> responsible = responsibleServices.get(profileId);
                if (responsible == null) {
                    // 该 Profile 无专用实例 → 使用「未绑定任何 Profile」的常规 RE
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

    /** 队列级运行时配置（目前主要用于是否把消息复制到全部分区） */
    @Data
    public static class QueueConfig {
        private boolean duplicateMsgToAllPartitions;

        public QueueConfig(QueueRoutingInfo queueRoutingInfo) {
            this.duplicateMsgToAllPartitions = queueRoutingInfo.isDuplicateMsgToAllPartitions();
        }

    }

}
