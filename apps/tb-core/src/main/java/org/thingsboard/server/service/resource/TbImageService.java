package org.thingsboard.server.service.resource;

import org.thingsboard.server.common.data.*;
import org.thingsboard.server.dao.resource.ImageCacheKey;
import org.thingsboard.server.service.security.model.SecurityUser;

/**
 * Core 侧图片资源门面。
 * <p>
 * 负责图片保存/删除、ETag 本地缓存，以及变更后向其它 Core 节点广播缓存失效（经通知队列）。
 *
 * @see DefaultTbImageService
 */
public interface TbImageService {

    /**
     * 保存图片二进制及元数据。
     */
    TbResourceInfo save(TbResource image, User user) throws Exception;

    /**
     * 仅更新图片元数据（公开状态等），必要时驱逐公开图 ETag。
     */
    TbResourceInfo save(TbResourceInfo imageInfo, TbResourceInfo oldImageInfo, User user);

    /**
     * 删除图片；成功后驱逐本机与集群 ETag 缓存。
     */
    TbImageDeleteResult delete(TbResourceInfo imageInfo, User user, boolean force);

    /**
     * 读取本机 ETag 缓存。
     */
    String getETag(ImageCacheKey imageCacheKey);

    /**
     * 写入本机 ETag 缓存。
     */
    void putETag(ImageCacheKey imageCacheKey, String etag);

    /**
     * 驱逐指定键（及预览图）的 ETag。
     */
    void evictETags(ImageCacheKey imageCacheKey);

    /**
     * 从导出数据导入图片；已存在且 {@code checkExisting} 时仅校验读权限。
     */
    TbResourceInfo importImage(ResourceExportData imageData, boolean checkExisting, SecurityUser user) throws Exception;

}
