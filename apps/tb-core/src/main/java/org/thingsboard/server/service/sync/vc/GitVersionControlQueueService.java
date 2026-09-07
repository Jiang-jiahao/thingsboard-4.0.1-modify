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
import org.thingsboard.server.common.data.ExportableEntity;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.sync.ie.EntityExportData;
import org.thingsboard.server.common.data.sync.vc.*;
import org.thingsboard.server.common.data.sync.vc.request.create.VersionCreateRequest;
import org.thingsboard.server.gen.transport.TransportProtos.VersionControlResponseMsg;
import org.thingsboard.server.service.sync.vc.data.CommitGitRequest;

import java.util.List;

/**
 * tb-core 与独立 Git/VC 服务之间的消息队列。
 * <p>
 * 提交、列版本、取实体 JSON、初始化/测试/清理仓库等请求都封装为 protobuf，
 * 推送到 version-control 服务；响应按 requestId 完成对应 Future。大 JSON 会分块传输。
 */
public interface GitVersionControlQueueService {

    /**
     * 向 VC 服务发送 prepare commit 请求。
     */
    ListenableFuture<CommitGitRequest> prepareCommit(User user, VersionCreateRequest request);

    /**
     * 将实体 JSON 分块加入当前 commit。
     */
    ListenableFuture<Void> addToCommit(CommitGitRequest commit, EntityExportData<ExportableEntity<EntityId>> entityData);

    /**
     * OVERWRITE 策略下删除某实体类型在仓库中的全部文件。
     */
    ListenableFuture<Void> deleteAll(CommitGitRequest pendingCommit, EntityType entityType);

    /**
     * 将已准备的 commit push 到远程。
     */
    ListenableFuture<VersionCreationResult> push(CommitGitRequest commit);

    ListenableFuture<PageData<EntityVersion>> listVersions(TenantId tenantId, String branch, PageLink pageLink);

    ListenableFuture<PageData<EntityVersion>> listVersions(TenantId tenantId, String branch, EntityType entityType, PageLink pageLink);

    ListenableFuture<PageData<EntityVersion>> listVersions(TenantId tenantId, String branch, EntityId entityId, PageLink pageLink);

    ListenableFuture<List<VersionedEntityInfo>> listEntitiesAtVersion(TenantId tenantId, String versionId, EntityType entityType);

    ListenableFuture<List<VersionedEntityInfo>> listEntitiesAtVersion(TenantId tenantId, String versionId);

    ListenableFuture<List<BranchInfo>> listBranches(TenantId tenantId);

    /**
     * 读取某版本中单个实体的导出 JSON。
     */
    ListenableFuture<EntityExportData> getEntity(TenantId tenantId, String versionId, EntityId entityId);

    /**
     * 分页读取某版本中指定类型的实体 JSON。
     */
    ListenableFuture<List<EntityExportData>> getEntities(TenantId tenantId, String versionId, EntityType entityType, int offset, int limit);

    ListenableFuture<List<EntityVersionsDiff>> getVersionsDiff(TenantId tenantId, EntityType entityType, EntityId externalId, String versionId1, String versionId2);

    ListenableFuture<Void> initRepository(TenantId tenantId, RepositorySettings settings);

    ListenableFuture<Void> testRepository(TenantId tenantId, RepositorySettings settings);

    ListenableFuture<Void> clearRepository(TenantId tenantId);

    /**
     * 处理 VC 服务回写的响应消息，完成对应 Future。
     */
    void processResponse(VersionControlResponseMsg vcResponseMsg);
}
