package com.jnks.iot.server.controller;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.WidgetTypeId;
import com.jnks.iot.server.common.data.id.WidgetsBundleId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.security.Authority;
import com.jnks.iot.server.common.data.widget.WidgetsBundle;
import com.jnks.iot.server.common.data.widget.WidgetsBundleFilter;
import com.jnks.iot.server.config.annotations.ApiOperation;
import com.jnks.iot.server.dao.resource.ImageService;
import com.jnks.iot.server.service.entitiy.widgets.bundle.JnksIotWidgetsBundleService;
import com.jnks.iot.server.service.security.permission.Operation;
import com.jnks.iot.server.service.security.permission.Resource;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static com.jnks.iot.server.controller.ControllerConstants.AVAILABLE_FOR_ANY_AUTHORIZED_USER;
import static com.jnks.iot.server.controller.ControllerConstants.INLINE_IMAGES;
import static com.jnks.iot.server.controller.ControllerConstants.INLINE_IMAGES_DESCRIPTION;
import static com.jnks.iot.server.controller.ControllerConstants.PAGE_DATA_PARAMETERS;
import static com.jnks.iot.server.controller.ControllerConstants.PAGE_NUMBER_DESCRIPTION;
import static com.jnks.iot.server.controller.ControllerConstants.PAGE_SIZE_DESCRIPTION;
import static com.jnks.iot.server.controller.ControllerConstants.SORT_ORDER_DESCRIPTION;
import static com.jnks.iot.server.controller.ControllerConstants.SORT_PROPERTY_DESCRIPTION;
import static com.jnks.iot.server.controller.ControllerConstants.SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH;
import static com.jnks.iot.server.controller.ControllerConstants.UUID_WIKI_LINK;
import static com.jnks.iot.server.controller.ControllerConstants.WIDGET_BUNDLE_ID_PARAM_DESCRIPTION;
import static com.jnks.iot.server.controller.ControllerConstants.WIDGET_BUNDLE_TEXT_SEARCH_DESCRIPTION;

/**
 * 部件包（Widget Bundle）CRUD 与部件列表维护 REST 入口。
 * <p>
 * 仅在 jnks-iot-core 模块（Core / Monolith）中生效。系统管理员维护系统级部件包；
 * 租户管理员维护本租户部件包。部件包按类型或场景分组部件。
 */
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class WidgetsBundleController extends BaseController {

    private final JnksIotWidgetsBundleService jnksIotWidgetsBundleService;
    private final ImageService imageService;

    private static final String WIDGET_BUNDLE_DESCRIPTION = "Widget Bundle represents a group(bundle) of widgets. Widgets are grouped into bundle by type or use case. ";
    private static final String FULL_SEARCH_PARAM_DESCRIPTION = "Optional boolean parameter indicating extended search of widget bundles by description and by name / description of related widget types";
    private static final String SCADA_FIRST_PARAM_DESCRIPTION = "Optional boolean parameter indicating whether to fetch widgets bundles with SCADA symbols first. Works only when fullSearch parameter is enabled";
    private static final String TENANT_BUNDLES_ONLY_DESCRIPTION = "Optional boolean parameter to include only tenant-level bundles without system";

    /**
     * 按 ID 读取部件包；可选内联图片。
     */
    @ApiOperation(value = "Get Widget Bundle (getWidgetsBundleById)",
            notes = "Get the Widget Bundle based on the provided Widget Bundle Id. " + WIDGET_BUNDLE_DESCRIPTION + AVAILABLE_FOR_ANY_AUTHORIZED_USER)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/widgetsBundle/{widgetsBundleId}", method = RequestMethod.GET)
    @ResponseBody
    public WidgetsBundle getWidgetsBundleById(
            @Parameter(description = WIDGET_BUNDLE_ID_PARAM_DESCRIPTION, required = true)
            @PathVariable("widgetsBundleId") String strWidgetsBundleId,
            @Parameter(description = INLINE_IMAGES_DESCRIPTION)
            @RequestParam(value = INLINE_IMAGES, required = false) boolean inlineImages) throws JnksIotException {
        checkParameter("widgetsBundleId", strWidgetsBundleId);
        WidgetsBundleId widgetsBundleId = new WidgetsBundleId(toUUID(strWidgetsBundleId));
        var result = checkWidgetsBundleId(widgetsBundleId, Operation.READ);
        if (inlineImages) {
            result = imageService.inlineImage(result);
        }
        return result;
    }

    /**
     * 创建或更新部件包；同一租户下 alias 唯一。
     */
    @ApiOperation(value = "Create Or Update Widget Bundle (saveWidgetsBundle)",
            notes = "Create or update the Widget Bundle. " + WIDGET_BUNDLE_DESCRIPTION + " " +
                    "When creating the bundle, platform generates Widget Bundle Id as " + UUID_WIKI_LINK +
                    "The newly created Widget Bundle Id will be present in the response. " +
                    "Specify existing Widget Bundle id to update the Widget Bundle. " +
                    "Referencing non-existing Widget Bundle Id will cause 'Not Found' error." +
                    "\n\nWidget Bundle alias is unique in the scope of tenant. " +
                    "Special Tenant Id '13814000-1dd2-11b2-8080-808080808080' is automatically used if the create bundle request is sent by user with 'SYS_ADMIN' authority." +
                    "Remove 'id', 'tenantId' from the request body example (below) to create new Widgets Bundle entity." +
                    SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    @RequestMapping(value = "/widgetsBundle", method = RequestMethod.POST)
    @ResponseBody
    public WidgetsBundle saveWidgetsBundle(
            @Parameter(description = "A JSON value representing the Widget Bundle.", required = true)
            @RequestBody WidgetsBundle widgetsBundle) throws Exception {
        var currentUser = getCurrentUser();
        if (Authority.SYS_ADMIN.equals(currentUser.getAuthority())) {
            widgetsBundle.setTenantId(TenantId.SYS_TENANT_ID);
        } else {
            widgetsBundle.setTenantId(currentUser.getTenantId());
        }

        checkEntity(widgetsBundle.getId(), widgetsBundle, Resource.WIDGETS_BUNDLE);

        return jnksIotWidgetsBundleService.save(widgetsBundle, currentUser);
    }

    /**
     * 按部件类型 ID 有序列表更新部件包内的部件。
     */
    @ApiOperation(value = "Update widgets bundle widgets types list (updateWidgetsBundleWidgetTypes)",
            notes = "Updates widgets bundle widgets list." + SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    @RequestMapping(value = "/widgetsBundle/{widgetsBundleId}/widgetTypes", method = RequestMethod.POST)
    @ResponseStatus(value = HttpStatus.OK)
    public void updateWidgetsBundleWidgetTypes(
            @Parameter(description = WIDGET_BUNDLE_ID_PARAM_DESCRIPTION, required = true)
            @PathVariable("widgetsBundleId") String strWidgetsBundleId,
            @Parameter(description = "Ordered list of widget type Ids to be included by widgets bundle")
            @RequestBody List<String> strWidgetTypeIds) throws Exception {
        checkParameter("widgetsBundleId", strWidgetsBundleId);
        WidgetsBundleId widgetsBundleId = new WidgetsBundleId(toUUID(strWidgetsBundleId));
        checkNotNull(strWidgetTypeIds);
        Set<WidgetTypeId> widgetTypeIds = new LinkedHashSet<>();
        var currentUser = getCurrentUser();
        TenantId tenantId = currentUser.getTenantId();
        for (String strWidgetTypeId : strWidgetTypeIds) {
            WidgetTypeId widgetTypeId = new WidgetTypeId(toUUID(strWidgetTypeId));
            if (!widgetTypeIds.contains(widgetTypeId) &&
                    widgetTypeService.widgetTypeExistsByTenantIdAndWidgetTypeId(tenantId, widgetTypeId)) {
                widgetTypeIds.add(widgetTypeId);
            }
        }
        jnksIotWidgetsBundleService.updateWidgetsBundleWidgetTypes(widgetsBundleId, new ArrayList<>(widgetTypeIds), currentUser);
    }

    /**
     * 按部件类型 FQN 有序列表更新部件包内的部件。
     */
    @ApiOperation(value = "Update widgets bundle widgets list from widget type FQNs list (updateWidgetsBundleWidgetFqns)",
            notes = "Updates widgets bundle widgets list from widget type FQNs list." + SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    @RequestMapping(value = "/widgetsBundle/{widgetsBundleId}/widgetTypeFqns", method = RequestMethod.POST)
    @ResponseStatus(value = HttpStatus.OK)
    public void updateWidgetsBundleWidgetFqns(
            @Parameter(description = WIDGET_BUNDLE_ID_PARAM_DESCRIPTION, required = true)
            @PathVariable("widgetsBundleId") String strWidgetsBundleId,
            @Parameter(description = "Ordered list of widget type FQNs to be included by widgets bundle")
            @RequestBody List<String> widgetTypeFqns) throws Exception {
        checkParameter("widgetsBundleId", strWidgetsBundleId);
        WidgetsBundleId widgetsBundleId = new WidgetsBundleId(toUUID(strWidgetsBundleId));
        checkNotNull(widgetTypeFqns);
        var currentUser = getCurrentUser();
        jnksIotWidgetsBundleService.updateWidgetsBundleWidgetFqns(widgetsBundleId, widgetTypeFqns, currentUser);
    }

    /**
     * 删除部件包。
     */
    @ApiOperation(value = "Delete widgets bundle (deleteWidgetsBundle)",
            notes = "Deletes the widget bundle. Referencing non-existing Widget Bundle Id will cause an error." + SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    @RequestMapping(value = "/widgetsBundle/{widgetsBundleId}", method = RequestMethod.DELETE)
    @ResponseStatus(value = HttpStatus.OK)
    public void deleteWidgetsBundle(
            @Parameter(description = WIDGET_BUNDLE_ID_PARAM_DESCRIPTION, required = true)
            @PathVariable("widgetsBundleId") String strWidgetsBundleId) throws JnksIotException {
        checkParameter("widgetsBundleId", strWidgetsBundleId);
        WidgetsBundleId widgetsBundleId = new WidgetsBundleId(toUUID(strWidgetsBundleId));
        WidgetsBundle widgetsBundle = checkWidgetsBundleId(widgetsBundleId, Operation.DELETE);
        jnksIotWidgetsBundleService.delete(widgetsBundle, getCurrentUser());
    }

    /**
     * 分页查询当前用户可见的部件包。
     */
    @ApiOperation(value = "Get Widget Bundles (getWidgetsBundles)",
            notes = "Returns a page of Widget Bundle objects available for current user. " + WIDGET_BUNDLE_DESCRIPTION + " " +
                    PAGE_DATA_PARAMETERS + AVAILABLE_FOR_ANY_AUTHORIZED_USER)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/widgetsBundles", params = {"pageSize", "page"}, method = RequestMethod.GET)
    @ResponseBody
    public PageData<WidgetsBundle> getWidgetsBundles(
            @Parameter(description = PAGE_SIZE_DESCRIPTION, required = true)
            @RequestParam int pageSize,
            @Parameter(description = PAGE_NUMBER_DESCRIPTION, required = true)
            @RequestParam int page,
            @Parameter(description = WIDGET_BUNDLE_TEXT_SEARCH_DESCRIPTION)
            @RequestParam(required = false) String textSearch,
            @Parameter(description = SORT_PROPERTY_DESCRIPTION, schema = @Schema(allowableValues = {"createdTime", "title", "tenantId"}))
            @RequestParam(required = false) String sortProperty,
            @Parameter(description = SORT_ORDER_DESCRIPTION, schema = @Schema(allowableValues = {"ASC", "DESC"}))
            @RequestParam(required = false) String sortOrder,
            @Parameter(description = TENANT_BUNDLES_ONLY_DESCRIPTION)
            @RequestParam(required = false) Boolean tenantOnly,
            @Parameter(description = FULL_SEARCH_PARAM_DESCRIPTION)
            @RequestParam(required = false) Boolean fullSearch,
            @Parameter(description = SCADA_FIRST_PARAM_DESCRIPTION)
            @RequestParam(required = false) Boolean scadaFirst) throws JnksIotException {
        PageLink pageLink = createPageLink(pageSize, page, textSearch, sortProperty, sortOrder);
        WidgetsBundleFilter widgetsBundleFilter = WidgetsBundleFilter.builder()
                .tenantId(getTenantId())
                .fullSearch(fullSearch != null && fullSearch)
                .scadaFirst(scadaFirst != null && scadaFirst)
                .build();
        if (Authority.SYS_ADMIN.equals(getCurrentUser().getAuthority())) {
            return checkNotNull(widgetsBundleService.findSystemWidgetsBundlesByPageLink(widgetsBundleFilter, pageLink));
        } else {
            if (tenantOnly != null && tenantOnly) {
                return checkNotNull(widgetsBundleService.findTenantWidgetsBundlesByTenantIdAndPageLink(widgetsBundleFilter, pageLink));
            } else {
                return checkNotNull(widgetsBundleService.findAllTenantWidgetsBundlesByTenantIdAndPageLink(widgetsBundleFilter, pageLink));
            }
        }
    }

    /**
     * 返回当前用户可见的全部部件包（不分页）。
     */
    @ApiOperation(value = "Get all Widget Bundles (getWidgetsBundles)",
            notes = "Returns an array of Widget Bundle objects that are available for current user." + WIDGET_BUNDLE_DESCRIPTION + " " + AVAILABLE_FOR_ANY_AUTHORIZED_USER)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/widgetsBundles", method = RequestMethod.GET)
    @ResponseBody
    public List<WidgetsBundle> getWidgetsBundles() throws JnksIotException {
        if (Authority.SYS_ADMIN.equals(getCurrentUser().getAuthority())) {
            return checkNotNull(widgetsBundleService.findSystemWidgetsBundles(getTenantId()));
        } else {
            TenantId tenantId = getCurrentUser().getTenantId();
            return checkNotNull(widgetsBundleService.findAllTenantWidgetsBundlesByTenantId(tenantId));
        }
    }

}
