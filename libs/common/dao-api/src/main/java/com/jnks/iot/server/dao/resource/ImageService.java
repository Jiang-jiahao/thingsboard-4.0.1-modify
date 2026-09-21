package com.jnks.iot.server.dao.resource;

import com.jnks.iot.server.common.data.Dashboard;
import com.jnks.iot.server.common.data.HasImage;
import com.jnks.iot.server.common.data.ResourceExportData;
import com.jnks.iot.server.common.data.ResourceSubType;
import com.jnks.iot.server.common.data.JnksIotImageDeleteResult;
import com.jnks.iot.server.common.data.JnksIotResource;
import com.jnks.iot.server.common.data.JnksIotResourceInfo;
import com.jnks.iot.server.common.data.id.JnksIotResourceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.widget.WidgetTypeDetails;

import java.util.Collection;

public interface ImageService {

    JnksIotResourceInfo saveImage(JnksIotResource image);

    JnksIotResourceInfo saveImageInfo(JnksIotResourceInfo imageInfo);

    JnksIotResourceInfo getImageInfoByTenantIdAndKey(TenantId tenantId, String key);

    JnksIotResourceInfo getPublicImageInfoByKey(String publicResourceKey);

    PageData<JnksIotResourceInfo> getImagesByTenantId(TenantId tenantId, ResourceSubType imageSubType, PageLink pageLink);

    PageData<JnksIotResourceInfo> getAllImagesByTenantId(TenantId tenantId, ResourceSubType imageSubType, PageLink pageLink);

    byte[] getImageData(TenantId tenantId, JnksIotResourceId imageId);

    byte[] getImagePreview(TenantId tenantId, JnksIotResourceId imageId);

    ResourceExportData exportImage(JnksIotResourceInfo imageInfo);

    JnksIotResource toImage(TenantId tenantId, ResourceExportData imageData, boolean checkExisting);

    JnksIotImageDeleteResult deleteImage(JnksIotResourceInfo imageInfo, boolean force);

    String calculateImageEtag(byte[] imageData);

    JnksIotResourceInfo findSystemOrTenantImageByEtag(TenantId tenantId, String etag);

    boolean replaceBase64WithImageUrl(HasImage entity, String type);

    boolean updateImagesUsage(Dashboard dashboard);

    boolean updateImagesUsage(WidgetTypeDetails widgetType);

    <T extends HasImage> T inlineImage(T entity);

    Collection<JnksIotResourceInfo> getUsedImages(Dashboard dashboard);

    Collection<JnksIotResourceInfo> getUsedImages(WidgetTypeDetails widgetTypeDetails);

    JnksIotResourceInfo createOrUpdateSystemImage(String resourceKey, byte[] data);

}
