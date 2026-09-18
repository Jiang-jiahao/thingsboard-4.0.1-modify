package org.thingsboard.server.service.resource;

import org.thingsboard.server.common.data.*;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.lwm2m.LwM2mObject;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.widget.WidgetTypeDetails;
import org.thingsboard.server.service.security.model.SecurityUser;

import java.util.List;

/**
 * Core 侧通用资源（非图片）门面。
 * <p>
 * 负责 LwM2M 模型等资源的保存/删除审计、导出仪表板与部件所用资源，以及导入时的权限校验。
 * 图片类型走 {@link TbImageService}。
 *
 * @see DefaultTbResourceService
 */
public interface TbResourceService {

    /**
     * 保存资源（无操作用户上下文）。
     */
    default TbResourceInfo save(TbResource entity) throws Exception {
        return save(entity, null);
    }

    /**
     * 保存资源并记录实体动作审计。
     */
    TbResourceInfo save(TbResource entity, SecurityUser user) throws Exception;

    /**
     * 删除资源；{@code force} 为 true 时忽略引用检查。
     */
    TbResourceDeleteResult delete(TbResourceInfo entity, boolean force, User user);

    /**
     * 按 LwM2M objectId 列表查询并转换为对象模型。
     */
    List<LwM2mObject> findLwM2mObject(TenantId tenantId,
                                      String sortOrder,
                                      String sortProperty,
                                      String[] objectIds);

    /**
     * 分页查询租户 LwM2M 模型并转换为对象模型。
     */
    List<LwM2mObject> findLwM2mObjectPage(TenantId tenantId,
                                          String sortProperty,
                                          String sortOrder,
                                          PageLink pageLink);

    /**
     * 导出仪表板引用的图片与其它资源。
     */
    List<ResourceExportData> exportResources(Dashboard dashboard, SecurityUser user) throws ThingsboardException;

    /**
     * 导出部件类型引用的图片与其它资源。
     */
    List<ResourceExportData> exportResources(WidgetTypeDetails widgetTypeDetails, SecurityUser user) throws ThingsboardException;

    /**
     * 导入资源列表：图片走 {@link TbImageService}，其余走本服务。
     */
    void importResources(List<ResourceExportData> resources, SecurityUser user) throws Exception;

}
