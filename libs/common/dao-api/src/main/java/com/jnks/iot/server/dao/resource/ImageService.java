package com.jnks.iot.server.dao.resource;

import com.jnks.iot.server.common.data.Dashboard;
import com.jnks.iot.server.common.data.HasImage;
import com.jnks.iot.server.common.data.ResourceExportData;
import com.jnks.iot.server.common.data.ResourceSubType;
import com.jnks.iot.server.common.data.TbImageDeleteResult;
import com.jnks.iot.server.common.data.TbResource;
import com.jnks.iot.server.common.data.TbResourceInfo;
import com.jnks.iot.server.common.data.id.TbResourceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.widget.WidgetTypeDetails;

import java.util.Collection;

public interface ImageService {

    TbResourceInfo saveImage(TbResource image);

    TbResourceInfo saveImageInfo(TbResourceInfo imageInfo);

    TbResourceInfo getImageInfoByTenantIdAndKey(TenantId tenantId, String key);

    TbResourceInfo getPublicImageInfoByKey(String publicResourceKey);

    PageData<TbResourceInfo> getImagesByTenantId(TenantId tenantId, ResourceSubType imageSubType, PageLink pageLink);

    PageData<TbResourceInfo> getAllImagesByTenantId(TenantId tenantId, ResourceSubType imageSubType, PageLink pageLink);

    byte[] getImageData(TenantId tenantId, TbResourceId imageId);

    byte[] getImagePreview(TenantId tenantId, TbResourceId imageId);

    ResourceExportData exportImage(TbResourceInfo imageInfo);

    TbResource toImage(TenantId tenantId, ResourceExportData imageData, boolean checkExisting);

    TbImageDeleteResult deleteImage(TbResourceInfo imageInfo, boolean force);

    String calculateImageEtag(byte[] imageData);

    TbResourceInfo findSystemOrTenantImageByEtag(TenantId tenantId, String etag);

    boolean replaceBase64WithImageUrl(HasImage entity, String type);

    boolean updateImagesUsage(Dashboard dashboard);

    boolean updateImagesUsage(WidgetTypeDetails widgetType);

    <T extends HasImage> T inlineImage(T entity);

    Collection<TbResourceInfo> getUsedImages(Dashboard dashboard);

    Collection<TbResourceInfo> getUsedImages(WidgetTypeDetails widgetTypeDetails);

    TbResourceInfo createOrUpdateSystemImage(String resourceKey, byte[] data);

}
