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
package org.thingsboard.server.service.sync.vc;

import com.google.common.util.concurrent.ListenableFuture;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.sync.vc.BranchInfo;
import org.thingsboard.server.common.data.sync.vc.EntityDataDiff;
import org.thingsboard.server.common.data.sync.vc.EntityDataInfo;
import org.thingsboard.server.common.data.sync.vc.EntityVersion;
import org.thingsboard.server.common.data.sync.vc.RepositorySettings;
import org.thingsboard.server.common.data.sync.vc.VersionCreationResult;
import org.thingsboard.server.common.data.sync.vc.VersionLoadResult;
import org.thingsboard.server.common.data.sync.vc.VersionedEntityInfo;
import org.thingsboard.server.common.data.sync.vc.request.create.VersionCreateRequest;
import org.thingsboard.server.common.data.sync.vc.request.load.VersionLoadRequest;

import java.util.List;
import java.util.UUID;

/**
 * 实体版本控制门面：把实体导出为 JSON 提交到 Git，或从某个版本加载并按冲突策略导入。
 * <p>
 * 提交走 Git 队列异步执行；加载支持单实体与按类型批量，批量导入按依赖顺序处理，
 * 并可 reimport 未解析完的引用、删除未出现在版本中的实体。
 */
public interface EntitiesVersionControlService {

    /**
     * 异步将实体导出为 JSON 并提交到 Git，返回任务 ID。
     */
    ListenableFuture<UUID> saveEntitiesVersion(User user, VersionCreateRequest request) throws Exception;

    /**
     * 查询创建版本任务的进度与结果。
     */
    VersionCreationResult getVersionCreateStatus(User user, UUID requestId) throws ThingsboardException;

    /**
     * 列出某实体在指定分支上的历史版本。
     */
    ListenableFuture<PageData<EntityVersion>> listEntityVersions(TenantId tenantId, String branch, EntityId externalId, PageLink pageLink) throws Exception;

    /**
     * 列出某实体类型在指定分支上的历史版本。
     */
    ListenableFuture<PageData<EntityVersion>> listEntityTypeVersions(TenantId tenantId, String branch, EntityType entityType, PageLink pageLink) throws Exception;

    /**
     * 列出指定分支上的全部版本。
     */
    ListenableFuture<PageData<EntityVersion>> listVersions(TenantId tenantId, String branch, PageLink pageLink) throws Exception;

    /**
     * 列出某版本中指定类型的实体。
     */
    ListenableFuture<List<VersionedEntityInfo>> listEntitiesAtVersion(TenantId tenantId, String versionId, EntityType entityType) throws Exception;

    /**
     * 列出某版本中的全部实体。
     */
    ListenableFuture<List<VersionedEntityInfo>> listAllEntitiesAtVersion(TenantId tenantId, String versionId) throws Exception;

    /**
     * 从 Git 版本加载实体并按冲突策略导入，返回任务 ID。
     */
    UUID loadEntitiesVersion(User user, VersionLoadRequest request) throws Exception;

    /**
     * 查询加载版本任务的进度与结果。
     */
    VersionLoadResult getVersionLoadStatus(User user, UUID requestId) throws ThingsboardException;

    /**
     * 将当前实体导出结果与指定 Git 版本做 diff。
     */
    ListenableFuture<EntityDataDiff> compareEntityDataToVersion(User user, EntityId entityId, String versionId) throws Exception;

    ListenableFuture<List<BranchInfo>> listBranches(TenantId tenantId) throws Exception;

    RepositorySettings getVersionControlSettings(TenantId tenantId);

    /**
     * 保存仓库设置并初始化远程仓库。
     */
    ListenableFuture<RepositorySettings> saveVersionControlSettings(TenantId tenantId, RepositorySettings versionControlSettings);

    /**
     * 删除仓库设置并清理仓库缓存。
     */
    ListenableFuture<Void> deleteVersionControlSettings(TenantId tenantId);

    /**
     * 用给定设置测试仓库连通性。
     */
    ListenableFuture<Void> checkVersionControlAccess(TenantId tenantId, RepositorySettings settings) throws Exception;

    /**
     * 单实体自动提交（需租户开启 auto-commit）。
     */
    ListenableFuture<UUID> autoCommit(User user, EntityId entityId) throws Exception;

    /**
     * 按类型批量自动提交（需租户开启 auto-commit）。
     */
    ListenableFuture<UUID> autoCommit(User user, EntityType entityType, List<UUID> entityIds) throws Exception;

    /**
     * 查看指定版本中该实体是否包含关系/属性/凭证/计算字段。
     */
    ListenableFuture<EntityDataInfo> getEntityDataInfo(User user, EntityId entityId, String versionId);

}
