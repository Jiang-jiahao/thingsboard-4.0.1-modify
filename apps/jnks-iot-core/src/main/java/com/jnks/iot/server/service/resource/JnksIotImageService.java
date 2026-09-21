package com.jnks.iot.server.service.resource;

import com.jnks.iot.server.common.data.*;
import com.jnks.iot.server.dao.resource.ImageCacheKey;
import com.jnks.iot.server.service.security.model.SecurityUser;

/**
 * Core 侧图片资源门面。
 * <p>
 * 负责图片保存/删除、ETag 本地缓存，以及变更后向其它 Core 节点广播缓存失效（经通知队列）。
 *
 * @see DefaultJnksIotImageService
 */
public interface JnksIotImageService {

    /**
     * 保存图片二进制及元数据。
     */
    JnksIotResourceInfo save(JnksIotResource image, User user) throws Exception;

    /**
     * 仅更新图片元数据（公开状态等），必要时驱逐公开图 ETag。
     */
    JnksIotResourceInfo save(JnksIotResourceInfo imageInfo, JnksIotResourceInfo oldImageInfo, User user);

    /**
     * 删除图片；成功后驱逐本机与集群 ETag 缓存。
     */
    JnksIotImageDeleteResult delete(JnksIotResourceInfo imageInfo, User user, boolean force);

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
    JnksIotResourceInfo importImage(ResourceExportData imageData, boolean checkExisting, SecurityUser user) throws Exception;

}
