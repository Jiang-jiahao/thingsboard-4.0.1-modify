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
package org.thingsboard.server.service.resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.thingsboard.server.cluster.TbClusterService;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.ImageDescriptor;
import org.thingsboard.server.common.data.ResourceExportData;
import org.thingsboard.server.common.data.TbImageDeleteResult;
import org.thingsboard.server.common.data.TbResource;
import org.thingsboard.server.common.data.TbResourceInfo;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.audit.ActionType;
import org.thingsboard.server.common.data.id.TbResourceId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.dao.resource.ImageCacheKey;
import org.thingsboard.server.dao.resource.ImageService;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.entitiy.AbstractTbEntityService;
import org.thingsboard.server.service.security.model.SecurityUser;
import org.thingsboard.server.service.security.permission.AccessControlService;
import org.thingsboard.server.service.security.permission.Operation;
import org.thingsboard.server.service.security.permission.Resource;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.thingsboard.server.common.data.StringUtils.isNotEmpty;

/**
 * {@link TbImageService} 默认实现。
 * <p>
 * 在 Core 实体服务层封装图片 DAO：保存/删除时记审计日志，ETag 变更后广播
 * {@code ResourceCacheInvalidateMsg} 到其它 Core 节点，保证集群 HTTP 缓存一致。
 *
 * @see TbImageService
 */
@Service
@Slf4j
@TbCoreComponent
public class DefaultTbImageService extends AbstractTbEntityService implements TbImageService {

    private final TbClusterService clusterService;
    private final ImageService imageService;
    private final AccessControlService accessControlService;
    private final Cache<ImageCacheKey, String> etagCache;

    public DefaultTbImageService(TbClusterService clusterService, ImageService imageService,
                                 AccessControlService accessControlService,
                                 @Value("${cache.image.etag.timeToLiveInMinutes:44640}") int cacheTtl,
                                 @Value("${cache.image.etag.maxSize:10000}") int cacheMaxSize) {
        this.clusterService = clusterService;
        this.imageService = imageService;
        this.accessControlService = accessControlService;
        this.etagCache = Caffeine.newBuilder()
                .expireAfterAccess(cacheTtl, TimeUnit.MINUTES)
                .maximumSize(cacheMaxSize)
                .build();
    }

    /**
     * 读取本机 ETag 缓存。
     */
    @Override
    public String getETag(ImageCacheKey imageCacheKey) {
        return etagCache.getIfPresent(imageCacheKey);
    }

    /**
     * 写入本机 ETag 缓存。
     */
    @Override
    public void putETag(ImageCacheKey imageCacheKey, String etag) {
        etagCache.put(imageCacheKey, etag);
    }

    /**
     * 驱逐原图 ETag；非公开图键时同时驱逐预览图。
     */
    @Override
    public void evictETags(ImageCacheKey imageCacheKey) {
        etagCache.invalidate(imageCacheKey);
        if (imageCacheKey.getPublicResourceKey() == null) {
            etagCache.invalidate(imageCacheKey.withPreview(true));
        }
    }

    /**
     * 保存图片；ETag 或公开状态变化时驱逐并广播集群缓存失效。
     */
    @Override
    public TbResourceInfo save(TbResource image, User user) throws Exception {
        ActionType actionType = image.getId() == null ? ActionType.ADDED : ActionType.UPDATED;
        TenantId tenantId = image.getTenantId();
        try {
            var oldEtag = getEtag(image);
            TbResourceInfo existingImage = null;
            if (image.getId() == null && isNotEmpty(image.getResourceKey())) {
                existingImage = imageService.getImageInfoByTenantIdAndKey(tenantId, image.getResourceKey());
                if (existingImage != null) {
                    image.setId(existingImage.getId());
                }
            }
            TbResourceInfo savedImage = imageService.saveImage(image);
            logEntityActionService.logEntityAction(tenantId, savedImage.getId(), savedImage, actionType, user);

            List<ImageCacheKey> toEvict = new ArrayList<>();
            if (oldEtag.isPresent()) {
                var newEtag = getEtag(savedImage);
                if (newEtag.isPresent() && !oldEtag.get().equals(newEtag.get())) {
                    toEvict.add(ImageCacheKey.forImage(tenantId, image.getResourceKey()));
                    if (image.isPublic()) {
                        toEvict.add(ImageCacheKey.forPublicImage(savedImage.getPublicResourceKey()));
                    }
                }
            }
            if (existingImage != null && image.isPublic() != existingImage.isPublic()) {
                toEvict.add(ImageCacheKey.forPublicImage(image.getPublicResourceKey()));
            }
            if (!toEvict.isEmpty()) {
                evictFromCache(tenantId, toEvict);
            }
            return savedImage;
        } catch (Exception e) {
            image.setData(null);
            logEntityActionService.logEntityAction(tenantId, emptyId(EntityType.TB_RESOURCE), new TbResourceInfo(image), actionType, user, e);
            throw e;
        }
    }

    private Optional<String> getEtag(TbResourceInfo image) throws JsonProcessingException {
        var descriptor = image.getDescriptor(ImageDescriptor.class);
        return Optional.ofNullable(descriptor != null ? descriptor.getEtag() : null);
    }

    private Optional<String> getPreviewEtag(TbResourceInfo image) throws JsonProcessingException {
        var descriptor = image.getDescriptor(ImageDescriptor.class);
        descriptor = descriptor != null ? descriptor.getPreviewDescriptor() : null;
        return Optional.ofNullable(descriptor != null ? descriptor.getEtag() : null);
    }

    /**
     * 更新图片元数据；公开状态翻转时驱逐公开图缓存。
     */
    @Override
    public TbResourceInfo save(TbResourceInfo imageInfo, TbResourceInfo oldImageInfo, User user) {
        TenantId tenantId = imageInfo.getTenantId();
        TbResourceId imageId = imageInfo.getId();
        try {
            imageInfo = imageService.saveImageInfo(imageInfo);
            logEntityActionService.logEntityAction(tenantId, imageId, imageInfo, ActionType.UPDATED, user);

            if (imageInfo.isPublic() != oldImageInfo.isPublic()) {
                evictFromCache(tenantId, List.of(ImageCacheKey.forPublicImage(imageInfo.getPublicResourceKey())));
            }
            return imageInfo;
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, imageId, imageInfo, ActionType.UPDATED, user, e);
            throw e;
        }
    }

    /**
     * 删除图片并在成功后驱逐本机与集群 ETag。
     */
    @Override
    public TbImageDeleteResult delete(TbResourceInfo imageInfo, User user, boolean force) {
        TenantId tenantId = imageInfo.getTenantId();
        TbResourceId imageId = imageInfo.getId();
        try {
            TbImageDeleteResult result = imageService.deleteImage(imageInfo, force);
            if (result.isSuccess()) {
                logEntityActionService.logEntityAction(tenantId, imageId, imageInfo, ActionType.DELETED, user, imageId.toString());

                List<ImageCacheKey> toEvict = new ArrayList<>();
                toEvict.add(ImageCacheKey.forImage(tenantId, imageInfo.getResourceKey()));
                if (imageInfo.isPublic()) {
                    toEvict.add(ImageCacheKey.forPublicImage(imageInfo.getPublicResourceKey()));
                }
                evictFromCache(tenantId, toEvict);
            }
            return result;
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, imageId, ActionType.DELETED, user, e, imageId.toString());
            throw e;
        }
    }

    /**
     * 导入图片：已存在则校验读权限，否则校验创建权限后保存。
     */
    @Override
    public TbResourceInfo importImage(ResourceExportData imageData, boolean checkExisting, SecurityUser user) throws Exception {
        TbResource image = imageService.toImage(user.getTenantId(), imageData, checkExisting);
        if (checkExisting && image.getId() != null) {
            accessControlService.checkPermission(user, Resource.TB_RESOURCE, Operation.READ, image.getId(), image);
            return image;
        } else {
            accessControlService.checkPermission(user, Resource.TB_RESOURCE, Operation.CREATE, null, image);
        }
        return save(image, user);
    }

    private void evictFromCache(TenantId tenantId, List<ImageCacheKey> toEvict) {
        toEvict.forEach(this::evictETags);
        clusterService.broadcastToCore(TransportProtos.ToCoreNotificationMsg.newBuilder()
                .setResourceCacheInvalidateMsg(TransportProtos.ResourceCacheInvalidateMsg.newBuilder()
                        .setTenantIdMSB(tenantId.getId().getMostSignificantBits())
                        .setTenantIdLSB(tenantId.getId().getLeastSignificantBits())
                        .addAllKeys(toEvict.stream().map(ImageCacheKey::toProto).collect(Collectors.toList()))
                        .build())
                .build());
    }

}
