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
package org.thingsboard.server.edqs.repo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.ObjectType;
import org.thingsboard.server.common.data.edqs.EdqsEvent;
import org.thingsboard.server.common.data.edqs.EdqsEventType;
import org.thingsboard.server.common.data.edqs.query.QueryResult;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.query.EntityCountQuery;
import org.thingsboard.server.common.data.query.EntityDataQuery;
import org.thingsboard.server.common.stats.EdqsStatsService;
import org.thingsboard.server.queue.edqs.EdqsComponent;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

/**
 * {@link EdqsRepository} 默认实现：按租户维护内存中的 {@link TenantRepo}，
 * 作为 EDQS 查询与事件写入的统一门面。
 * <p>
 * 职责概览：
 * <ul>
 *   <li>按 {@link TenantId} 懒创建并缓存 {@link TenantRepo}（实体、关系、属性、最新时序等内存索引）；</li>
 *   <li>接收 {@link EdqsEvent}：租户删除时移除整库，其它事件委托给对应 {@link TenantRepo}；</li>
 *   <li>执行实体计数 / 实体数据查询，并上报耗时统计；</li>
 *   <li>支持按条件或全量清理租户内存仓库（如分区变更后释放不再负责的租户）。</li>
 * </ul>
 */
@EdqsComponent
@AllArgsConstructor
@Service
@Slf4j
public class DefaultEdqsRepository implements EdqsRepository {

    /**
     * 租户 → 内存仓库映射；全局静态，进程内共享，按租户隔离索引数据。
     */
    @Getter
    private final static ConcurrentMap<TenantId, TenantRepo> repos = new ConcurrentHashMap<>();
    private final EdqsStatsService statsService;

    /**
     * 获取指定租户的内存仓库；若不存在则创建并放入 {@link #repos}。
     *
     * @param tenantId 租户 ID
     * @return 该租户对应的 {@link TenantRepo}
     */
    public TenantRepo get(TenantId tenantId) {
        return repos.computeIfAbsent(tenantId, id -> new TenantRepo(id, statsService));
    }

    /**
     * 处理一条 EDQS 事件。
     * <p>
     * 特殊路径：事件为「删除租户」时，直接从 {@link #repos} 移除该租户仓库并上报统计，
     * 不再进入 {@link TenantRepo}；其它事件（增删改实体/关系/属性等）委托给对应租户仓库。
     *
     * @param event 已反序列化的 EDQS 事件
     */
    @Override
    public void processEvent(EdqsEvent event) {
        if (event.getEventType() == EdqsEventType.DELETED && event.getObjectType() == ObjectType.TENANT) {
            log.info("Tenant {} deleted", event.getTenantId());
            repos.remove(event.getTenantId());
            statsService.reportRemoved(ObjectType.TENANT);
        } else {
            get(event.getTenantId()).processEvent(event);
        }
    }

    /**
     * 按实体计数查询统计匹配实体数量，并上报查询耗时。
     *
     * @param tenantId              租户
     * @param customerId            客户（权限范围；可为 null）
     * @param query                 计数查询条件
     * @param ignorePermissionCheck 是否跳过权限校验
     * @return 匹配实体数
     */
    @Override
    public long countEntitiesByQuery(TenantId tenantId, CustomerId customerId, EntityCountQuery query, boolean ignorePermissionCheck) {
        long startNs = System.nanoTime();
        long result = get(tenantId).countEntitiesByQuery(customerId, query, ignorePermissionCheck);
        statsService.reportEdqsCountQuery(tenantId, query, System.nanoTime() - startNs);
        return result;
    }

    /**
     * 按实体数据查询分页检索结果，并上报查询耗时。
     *
     * @param tenantId              租户
     * @param customerId            客户（权限范围；可为 null）
     * @param query                 实体数据查询（过滤、排序、分页、返回字段等）
     * @param ignorePermissionCheck 是否跳过权限校验
     * @return 分页查询结果
     */
    @Override
    public PageData<QueryResult> findEntityDataByQuery(TenantId tenantId, CustomerId customerId,
                                                       EntityDataQuery query, boolean ignorePermissionCheck) {
        long startNs = System.nanoTime();
        var result = get(tenantId).findEntityDataByQuery(customerId, query, ignorePermissionCheck);
        statsService.reportEdqsDataQuery(tenantId, query, System.nanoTime() - startNs);
        return result;
    }

    /**
     * 按条件移除租户内存仓库（例如分区变更后，本节点不再负责的租户）。
     *
     * @param predicate 为 true 的租户会从 {@link #repos} 中删除
     */
    @Override
    public void clearIf(Predicate<TenantId> predicate) {
        repos.keySet().removeIf(predicate);
    }

    /**
     * 清空全部租户内存仓库。
     */
    @Override
    public void clear() {
        repos.clear();
    }

}
