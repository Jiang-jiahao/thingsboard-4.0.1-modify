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
package org.thingsboard.server.controller;

import com.google.common.util.concurrent.ListenableFuture;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
import org.thingsboard.server.common.data.Customer;
import org.thingsboard.server.common.data.EntitySubtype;
import org.thingsboard.server.common.data.EntityView;
import org.thingsboard.server.common.data.EntityViewInfo;
import org.thingsboard.server.common.data.edge.Edge;
import org.thingsboard.server.common.data.entityview.EntityViewSearchQuery;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.EdgeId;
import org.thingsboard.server.common.data.id.EntityViewId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.page.TimePageLink;
import org.thingsboard.server.config.annotations.ApiOperation;
import org.thingsboard.server.dao.exception.IncorrectParameterException;
import org.thingsboard.server.dao.model.ModelConstants;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.entitiy.entityview.TbEntityViewService;
import org.thingsboard.server.service.security.model.SecurityUser;
import org.thingsboard.server.service.security.permission.Operation;
import org.thingsboard.server.service.security.permission.Resource;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.thingsboard.server.controller.ControllerConstants.CUSTOMER_ID;
import static org.thingsboard.server.controller.ControllerConstants.CUSTOMER_ID_PARAM_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.EDGE_ASSIGN_ASYNC_FIRST_STEP_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.EDGE_ASSIGN_RECEIVE_STEP_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.EDGE_UNASSIGN_ASYNC_FIRST_STEP_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.EDGE_UNASSIGN_RECEIVE_STEP_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.ENTITY_VIEW_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.ENTITY_VIEW_ID_PARAM_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.ENTITY_VIEW_INFO_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.ENTITY_VIEW_TEXT_SEARCH_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.ENTITY_VIEW_TYPE;
import static org.thingsboard.server.controller.ControllerConstants.MODEL_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.PAGE_DATA_PARAMETERS;
import static org.thingsboard.server.controller.ControllerConstants.PAGE_NUMBER_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.PAGE_SIZE_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.SORT_ORDER_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.SORT_PROPERTY_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.TENANT_AUTHORITY_PARAGRAPH;
import static org.thingsboard.server.controller.ControllerConstants.TENANT_OR_CUSTOMER_AUTHORITY_PARAGRAPH;
import static org.thingsboard.server.controller.EdgeController.EDGE_ID;

/**
 * 实体视图（Entity View）REST 入口。
 * <p>
 * 实体视图是设备/资产遥测与属性的只读窗口，可分配给客户或公开客户，也可同步到 Edge。
 * 仅在 {@link TbCoreComponent} 中生效。
 * <p>
 * <b>URL 前缀：</b>{@code /api}。路径如 {@code /entityView}、{@code /tenant/entityViews}、
 * {@code /customer/{customerId}/entityViews}、{@code /edge/{edgeId}/entityView/{entityViewId}}。
 * <p>
 * <b>权限：</b>读写对 TENANT_ADMIN、CUSTOMER_USER 开放（客户仅能操作已分配的视图）；
 * 删除、分配客户/公开/Edge 仅 TENANT_ADMIN。
 * <p>
 * <b>下游：</b>写/分配/解绑 {@link TbEntityViewService}；查询走基类 {@code entityViewService}。
 *
 * @see TbEntityViewService
 */
@RestController
@TbCoreComponent
@RequiredArgsConstructor
@RequestMapping("/api")
@Slf4j
public class EntityViewController extends BaseController {

    public final TbEntityViewService tbEntityViewService;

    public static final String ENTITY_VIEW_ID = "entityViewId";

    /**
     * 按 Id 读取完整实体视图。
     */
    @ApiOperation(value = "Get entity view (getEntityViewById)",
            notes = "Fetch the EntityView object based on the provided entity view id. "
                    + ENTITY_VIEW_DESCRIPTION + MODEL_DESCRIPTION + TENANT_OR_CUSTOMER_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/entityView/{entityViewId}", method = RequestMethod.GET)
    @ResponseBody
    public EntityView getEntityViewById(
            @Parameter(description = ENTITY_VIEW_ID_PARAM_DESCRIPTION)
            @PathVariable(ENTITY_VIEW_ID) String strEntityViewId) throws ThingsboardException {
        checkParameter(ENTITY_VIEW_ID, strEntityViewId);
        return checkEntityViewId(new EntityViewId(toUUID(strEntityViewId)), Operation.READ);
    }

    /**
     * 按 Id 读取实体视图摘要（含客户标题等）。
     */
    @ApiOperation(value = "Get Entity View info (getEntityViewInfoById)",
            notes = "Fetch the Entity View info object based on the provided Entity View Id. "
                    + ENTITY_VIEW_INFO_DESCRIPTION + MODEL_DESCRIPTION + TENANT_OR_CUSTOMER_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/entityView/info/{entityViewId}", method = RequestMethod.GET)
    @ResponseBody
    public EntityViewInfo getEntityViewInfoById(
            @Parameter(description = ENTITY_VIEW_ID_PARAM_DESCRIPTION)
            @PathVariable(ENTITY_VIEW_ID) String strEntityViewId) throws ThingsboardException {
        checkParameter(ENTITY_VIEW_ID, strEntityViewId);
        EntityViewId entityViewId = new EntityViewId(toUUID(strEntityViewId));
        return checkEntityViewInfoId(entityViewId, Operation.READ);
    }

    /**
     * 创建或更新实体视图。无 {@code id} 时校验 CREATE，有 id 时校验 WRITE。
     */
    @ApiOperation(value = "Save or update entity view (saveEntityView)",
            notes = ENTITY_VIEW_DESCRIPTION + MODEL_DESCRIPTION +
                    "Remove 'id', 'tenantId' and optionally 'customerId' from the request body example (below) to create new Entity View entity." +
                    TENANT_OR_CUSTOMER_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/entityView", method = RequestMethod.POST)
    @ResponseBody
    public EntityView saveEntityView(
            @Parameter(description = "A JSON object representing the entity view.")
            @RequestBody EntityView entityView) throws Exception {
        entityView.setTenantId(getCurrentUser().getTenantId());
        EntityView existingEntityView = null;
        if (entityView.getId() == null) {
            accessControlService
                    .checkPermission(getCurrentUser(), Resource.ENTITY_VIEW, Operation.CREATE, null, entityView);
        } else {
            existingEntityView = checkEntityViewId(entityView.getId(), Operation.WRITE);
        }
        return tbEntityViewService.save(entityView, existingEntityView, getCurrentUser());
    }

    /**
     * 按 Id 删除实体视图。
     */
    @ApiOperation(value = "Delete entity view (deleteEntityView)",
            notes = "Delete the EntityView object based on the provided entity view id. "
                    + TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAuthority('TENANT_ADMIN')")
    @RequestMapping(value = "/entityView/{entityViewId}", method = RequestMethod.DELETE)
    @ResponseStatus(value = HttpStatus.OK)
    public void deleteEntityView(
            @Parameter(description = ENTITY_VIEW_ID_PARAM_DESCRIPTION)
            @PathVariable(ENTITY_VIEW_ID) String strEntityViewId) throws ThingsboardException {
        checkParameter(ENTITY_VIEW_ID, strEntityViewId);
        EntityViewId entityViewId = new EntityViewId(toUUID(strEntityViewId));
        EntityView entityView = checkEntityViewId(entityViewId, Operation.DELETE);
        tbEntityViewService.delete(entityView, getCurrentUser());
    }

    /**
     * 按名称读取当前租户的实体视图。
     */
    @ApiOperation(value = "Get Entity View by name (getTenantEntityView)",
            notes = "Fetch the Entity View object based on the tenant id and entity view name. " + TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAuthority('TENANT_ADMIN')")
    @RequestMapping(value = "/tenant/entityViews", params = {"entityViewName"}, method = RequestMethod.GET)
    @ResponseBody
    public EntityView getTenantEntityView(
            @Parameter(description = "Entity View name")
            @RequestParam String entityViewName) throws ThingsboardException {
        TenantId tenantId = getCurrentUser().getTenantId();
        return checkNotNull(entityViewService.findEntityViewByTenantIdAndName(tenantId, entityViewName));
    }

    /**
     * 把实体视图分配给客户，之后该客户可查询此视图。
     */
    @ApiOperation(value = "Assign Entity View to customer (assignEntityViewToCustomer)",
            notes = "Creates assignment of the Entity View to customer. Customer will be able to query Entity View afterwards." + TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAuthority('TENANT_ADMIN')")
    @RequestMapping(value = "/customer/{customerId}/entityView/{entityViewId}", method = RequestMethod.POST)
    @ResponseBody
    public EntityView assignEntityViewToCustomer(
            @Parameter(description = CUSTOMER_ID_PARAM_DESCRIPTION)
            @PathVariable(CUSTOMER_ID) String strCustomerId,
            @Parameter(description = ENTITY_VIEW_ID_PARAM_DESCRIPTION)
            @PathVariable(ENTITY_VIEW_ID) String strEntityViewId) throws ThingsboardException {
        checkParameter(CUSTOMER_ID, strCustomerId);
        checkParameter(ENTITY_VIEW_ID, strEntityViewId);

        CustomerId customerId = new CustomerId(toUUID(strCustomerId));
        Customer customer = checkCustomerId(customerId, Operation.READ);

        EntityViewId entityViewId = new EntityViewId(toUUID(strEntityViewId));
        checkEntityViewId(entityViewId, Operation.ASSIGN_TO_CUSTOMER);

        return tbEntityViewService.assignEntityViewToCustomer(getTenantId(), entityViewId, customer, getCurrentUser());
    }

    /**
     * 取消实体视图与客户的分配。未分配时抛错。
     */
    @ApiOperation(value = "Unassign Entity View from customer (unassignEntityViewFromCustomer)",
            notes = "Clears assignment of the Entity View to customer. Customer will not be able to query Entity View afterwards." + TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAuthority('TENANT_ADMIN')")
    @RequestMapping(value = "/customer/entityView/{entityViewId}", method = RequestMethod.DELETE)
    @ResponseBody
    public EntityView unassignEntityViewFromCustomer(
            @Parameter(description = ENTITY_VIEW_ID_PARAM_DESCRIPTION)
            @PathVariable(ENTITY_VIEW_ID) String strEntityViewId) throws ThingsboardException {
        checkParameter(ENTITY_VIEW_ID, strEntityViewId);
        EntityViewId entityViewId = new EntityViewId(toUUID(strEntityViewId));
        EntityView entityView = checkEntityViewId(entityViewId, Operation.UNASSIGN_FROM_CUSTOMER);
        if (entityView.getCustomerId() == null || entityView.getCustomerId().getId().equals(ModelConstants.NULL_UUID)) {
            throw new IncorrectParameterException("Entity View isn't assigned to any customer!");
        }

        Customer customer = checkCustomerId(entityView.getCustomerId(), Operation.READ);

        return tbEntityViewService.unassignEntityViewFromCustomer(getTenantId(), entityViewId, customer, getCurrentUser());
    }

    /**
     * 分页列出分配给指定客户的实体视图，可按类型过滤。
     */
    @ApiOperation(value = "Get Customer Entity Views (getCustomerEntityViews)",
            notes = "Returns a page of Entity View objects assigned to customer. " +
                    PAGE_DATA_PARAMETERS + TENANT_OR_CUSTOMER_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/customer/{customerId}/entityViews", params = {"pageSize", "page"}, method = RequestMethod.GET)
    @ResponseBody
    public PageData<EntityView> getCustomerEntityViews(
            @Parameter(description = CUSTOMER_ID_PARAM_DESCRIPTION, required = true)
            @PathVariable(CUSTOMER_ID) String strCustomerId,
            @Parameter(description = PAGE_SIZE_DESCRIPTION, required = true)
            @RequestParam int pageSize,
            @Parameter(description = PAGE_NUMBER_DESCRIPTION, required = true)
            @RequestParam int page,
            @Parameter(description = ENTITY_VIEW_TYPE)
            @RequestParam(required = false) String type,
            @Parameter(description = ENTITY_VIEW_TEXT_SEARCH_DESCRIPTION)
            @RequestParam(required = false) String textSearch,
            @Parameter(description = SORT_PROPERTY_DESCRIPTION, schema = @Schema(allowableValues = {"createdTime", "name", "type"}))
            @RequestParam(required = false) String sortProperty,
            @Parameter(description = SORT_ORDER_DESCRIPTION, schema = @Schema(allowableValues = {"ASC", "DESC"}))
            @RequestParam(required = false) String sortOrder) throws ThingsboardException {
        checkParameter(CUSTOMER_ID, strCustomerId);
        TenantId tenantId = getCurrentUser().getTenantId();
        CustomerId customerId = new CustomerId(toUUID(strCustomerId));
        checkCustomerId(customerId, Operation.READ);
        PageLink pageLink = createPageLink(pageSize, page, textSearch, sortProperty, sortOrder);
        if (type != null && type.trim().length() > 0) {
            return checkNotNull(entityViewService.findEntityViewsByTenantIdAndCustomerIdAndType(tenantId, customerId, pageLink, type));
        } else {
            return checkNotNull(entityViewService.findEntityViewsByTenantIdAndCustomerId(tenantId, customerId, pageLink));
        }
    }

    /**
     * 分页列出分配给指定客户的实体视图摘要。
     */
    @ApiOperation(value = "Get Customer Entity View info (getCustomerEntityViewInfos)",
            notes = "Returns a page of Entity View info objects assigned to customer. " + ENTITY_VIEW_DESCRIPTION +
                    PAGE_DATA_PARAMETERS + TENANT_OR_CUSTOMER_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/customer/{customerId}/entityViewInfos", params = {"pageSize", "page"}, method = RequestMethod.GET)
    @ResponseBody
    public PageData<EntityViewInfo> getCustomerEntityViewInfos(
            @Parameter(description = CUSTOMER_ID_PARAM_DESCRIPTION, required = true)
            @PathVariable(CUSTOMER_ID) String strCustomerId,
            @Parameter(description = PAGE_SIZE_DESCRIPTION, required = true)
            @RequestParam int pageSize,
            @Parameter(description = PAGE_NUMBER_DESCRIPTION, required = true)
            @RequestParam int page,
            @Parameter(description = ENTITY_VIEW_TYPE)
            @RequestParam(required = false) String type,
            @Parameter(description = ENTITY_VIEW_TEXT_SEARCH_DESCRIPTION)
            @RequestParam(required = false) String textSearch,
            @Parameter(description = SORT_PROPERTY_DESCRIPTION, schema = @Schema(allowableValues = {"createdTime", "name", "type", "customerTitle"}))
            @RequestParam(required = false) String sortProperty,
            @Parameter(description = SORT_ORDER_DESCRIPTION, schema = @Schema(allowableValues = {"ASC", "DESC"}))
            @RequestParam(required = false) String sortOrder) throws ThingsboardException {
        checkParameter("customerId", strCustomerId);
        TenantId tenantId = getCurrentUser().getTenantId();
        CustomerId customerId = new CustomerId(toUUID(strCustomerId));
        checkCustomerId(customerId, Operation.READ);
        PageLink pageLink = createPageLink(pageSize, page, textSearch, sortProperty, sortOrder);
        if (type != null && type.trim().length() > 0) {
            return checkNotNull(entityViewService.findEntityViewInfosByTenantIdAndCustomerIdAndType(tenantId, customerId, type, pageLink));
        } else {
            return checkNotNull(entityViewService.findEntityViewInfosByTenantIdAndCustomerId(tenantId, customerId, pageLink));
        }
    }

    /**
     * 分页列出当前租户拥有的实体视图，可按类型过滤。
     */
    @ApiOperation(value = "Get Tenant Entity Views (getTenantEntityViews)",
            notes = "Returns a page of entity views owned by tenant. " + ENTITY_VIEW_DESCRIPTION +
                    PAGE_DATA_PARAMETERS + TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAuthority('TENANT_ADMIN')")
    @RequestMapping(value = "/tenant/entityViews", params = {"pageSize", "page"}, method = RequestMethod.GET)
    @ResponseBody
    public PageData<EntityView> getTenantEntityViews(
            @Parameter(description = PAGE_SIZE_DESCRIPTION, required = true)
            @RequestParam int pageSize,
            @Parameter(description = PAGE_NUMBER_DESCRIPTION, required = true)
            @RequestParam int page,
            @Parameter(description = ENTITY_VIEW_TYPE)
            @RequestParam(required = false) String type,
            @Parameter(description = ENTITY_VIEW_TEXT_SEARCH_DESCRIPTION)
            @RequestParam(required = false) String textSearch,
            @Parameter(description = SORT_PROPERTY_DESCRIPTION, schema = @Schema(allowableValues = {"createdTime", "name", "type"}))
            @RequestParam(required = false) String sortProperty,
            @Parameter(description = SORT_ORDER_DESCRIPTION, schema = @Schema(allowableValues = {"ASC", "DESC"}))
            @RequestParam(required = false) String sortOrder) throws ThingsboardException {
        TenantId tenantId = getCurrentUser().getTenantId();
        PageLink pageLink = createPageLink(pageSize, page, textSearch, sortProperty, sortOrder);

        if (type != null && type.trim().length() > 0) {
            return checkNotNull(entityViewService.findEntityViewByTenantIdAndType(tenantId, pageLink, type));
        } else {
            return checkNotNull(entityViewService.findEntityViewByTenantId(tenantId, pageLink));
        }
    }

    /**
     * 分页列出当前租户拥有的实体视图摘要。
     */
    @ApiOperation(value = "Get Tenant Entity Views (getTenantEntityViews)",
            notes = "Returns a page of entity views info owned by tenant. " + ENTITY_VIEW_DESCRIPTION +
                    PAGE_DATA_PARAMETERS + TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAuthority('TENANT_ADMIN')")
    @RequestMapping(value = "/tenant/entityViewInfos", params = {"pageSize", "page"}, method = RequestMethod.GET)
    @ResponseBody
    public PageData<EntityViewInfo> getTenantEntityViewInfos(
            @Parameter(description = PAGE_SIZE_DESCRIPTION, required = true)
            @RequestParam int pageSize,
            @Parameter(description = PAGE_NUMBER_DESCRIPTION, required = true)
            @RequestParam int page,
            @Parameter(description = ENTITY_VIEW_TYPE)
            @RequestParam(required = false) String type,
            @Parameter(description = ENTITY_VIEW_TEXT_SEARCH_DESCRIPTION)
            @RequestParam(required = false) String textSearch,
            @Parameter(description = SORT_PROPERTY_DESCRIPTION, schema = @Schema(allowableValues = {"createdTime", "name", "type", "customerTitle"}))
            @RequestParam(required = false) String sortProperty,
            @Parameter(description = SORT_ORDER_DESCRIPTION, schema = @Schema(allowableValues = {"ASC", "DESC"}))
            @RequestParam(required = false) String sortOrder) throws ThingsboardException {
        TenantId tenantId = getCurrentUser().getTenantId();
        PageLink pageLink = createPageLink(pageSize, page, textSearch, sortProperty, sortOrder);
        if (type != null && type.trim().length() > 0) {
            return checkNotNull(entityViewService.findEntityViewInfosByTenantIdAndType(tenantId, type, pageLink));
        } else {
            return checkNotNull(entityViewService.findEntityViewInfosByTenantId(tenantId, pageLink));
        }
    }

    /**
     * 按 {@link EntityViewSearchQuery} 查找与某实体相关的实体视图，结果再按 READ 过滤。
     */
    @ApiOperation(value = "Find related entity views (findByQuery)",
            notes = "Returns all entity views that are related to the specific entity. " +
                    "The entity id, relation type, entity view types, depth of the search, and other query parameters defined using complex 'EntityViewSearchQuery' object. " +
                    "See 'Model' tab of the Parameters for more info." + TENANT_OR_CUSTOMER_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/entityViews", method = RequestMethod.POST)
    @ResponseBody
    public List<EntityView> findByQuery(
            @Parameter(description = "The entity view search query JSON")
            @RequestBody EntityViewSearchQuery query) throws ThingsboardException, ExecutionException, InterruptedException {
        checkNotNull(query);
        checkNotNull(query.getParameters());
        checkNotNull(query.getEntityViewTypes());
        checkEntityId(query.getParameters().getEntityId(), Operation.READ);
        List<EntityView> entityViews = checkNotNull(entityViewService.findEntityViewsByQuery(getTenantId(), query).get());
        entityViews = entityViews.stream().filter(entityView -> {
            try {
                accessControlService.checkPermission(getCurrentUser(), Resource.ENTITY_VIEW, Operation.READ, entityView.getId(), entityView);
                return true;
            } catch (ThingsboardException e) {
                return false;
            }
        }).collect(Collectors.toList());
        return entityViews;
    }

    /**
     * 返回当前租户下已使用的实体视图类型集合。
     */
    @ApiOperation(value = "Get Entity View Types (getEntityViewTypes)",
            notes = "Returns a set of unique entity view types based on entity views that are either owned by the tenant or assigned to the customer which user is performing the request."
                    + TENANT_OR_CUSTOMER_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/entityView/types", method = RequestMethod.GET)
    @ResponseBody
    public List<EntitySubtype> getEntityViewTypes() throws ThingsboardException, ExecutionException, InterruptedException {
        SecurityUser user = getCurrentUser();
        TenantId tenantId = user.getTenantId();
        ListenableFuture<List<EntitySubtype>> entityViewTypes = entityViewService.findEntityViewTypesByTenantId(tenantId);
        return checkNotNull(entityViewTypes.get());
    }

    /**
     * 把实体视图分配给公开客户，未登录用户也可访问（用于嵌入公开仪表盘）。
     */
    @ApiOperation(value = "Make entity view publicly available (assignEntityViewToPublicCustomer)",
            notes = "Entity View will be available for non-authorized (not logged-in) users. " +
                    "This is useful to create dashboards that you plan to share/embed on a publicly available website. " +
                    "However, users that are logged-in and belong to different tenant will not be able to access the entity view." + TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAuthority('TENANT_ADMIN')")
    @RequestMapping(value = "/customer/public/entityView/{entityViewId}", method = RequestMethod.POST)
    @ResponseBody
    public EntityView assignEntityViewToPublicCustomer(
            @Parameter(description = ENTITY_VIEW_ID_PARAM_DESCRIPTION)
            @PathVariable(ENTITY_VIEW_ID) String strEntityViewId) throws ThingsboardException {
        checkParameter(ENTITY_VIEW_ID, strEntityViewId);
        EntityViewId entityViewId = new EntityViewId(toUUID(strEntityViewId));
        checkEntityViewId(entityViewId, Operation.ASSIGN_TO_CUSTOMER);
        return tbEntityViewService.assignEntityViewToPublicCustomer(getTenantId(), entityViewId, getCurrentUser());
    }

    /**
     * 把实体视图分配到 Edge，异步同步到远端边缘实例。
     */
    @ApiOperation(value = "Assign entity view to edge (assignEntityViewToEdge)",
            notes = "Creates assignment of an existing entity view to an instance of The Edge. " +
                    EDGE_ASSIGN_ASYNC_FIRST_STEP_DESCRIPTION +
                    "Second, remote edge service will receive a copy of assignment entity view " +
                    EDGE_ASSIGN_RECEIVE_STEP_DESCRIPTION +
                    "Third, once entity view will be delivered to edge service, it's going to be available for usage on remote edge instance.")
    @PreAuthorize("hasAuthority('TENANT_ADMIN')")
    @RequestMapping(value = "/edge/{edgeId}/entityView/{entityViewId}", method = RequestMethod.POST)
    @ResponseBody
    public EntityView assignEntityViewToEdge(@PathVariable(EDGE_ID) String strEdgeId,
                                             @PathVariable(ENTITY_VIEW_ID) String strEntityViewId) throws ThingsboardException {
        checkParameter(EDGE_ID, strEdgeId);
        checkParameter(ENTITY_VIEW_ID, strEntityViewId);

        EdgeId edgeId = new EdgeId(toUUID(strEdgeId));
        Edge edge = checkEdgeId(edgeId, Operation.READ);

        EntityViewId entityViewId = new EntityViewId(toUUID(strEntityViewId));
        checkEntityViewId(entityViewId, Operation.READ);

        return tbEntityViewService.assignEntityViewToEdge(getTenantId(), getCurrentUser().getCustomerId(),
                entityViewId, edge, getCurrentUser());
    }

    /**
     * 取消实体视图与 Edge 的分配，并通知远端删除本地副本。
     */
    @ApiOperation(value = "Unassign entity view from edge (unassignEntityViewFromEdge)",
            notes = "Clears assignment of the entity view to the edge. " +
                    EDGE_UNASSIGN_ASYNC_FIRST_STEP_DESCRIPTION +
                    "Second, remote edge service will receive an 'unassign' command to remove entity view " +
                    EDGE_UNASSIGN_RECEIVE_STEP_DESCRIPTION +
                    "Third, once 'unassign' command will be delivered to edge service, it's going to remove entity view locally.")
    @PreAuthorize("hasAuthority('TENANT_ADMIN')")
    @RequestMapping(value = "/edge/{edgeId}/entityView/{entityViewId}", method = RequestMethod.DELETE)
    @ResponseBody
    public EntityView unassignEntityViewFromEdge(@PathVariable(EDGE_ID) String strEdgeId,
                                                 @PathVariable(ENTITY_VIEW_ID) String strEntityViewId) throws ThingsboardException {
        checkParameter(EDGE_ID, strEdgeId);
        checkParameter(ENTITY_VIEW_ID, strEntityViewId);

        EdgeId edgeId = new EdgeId(toUUID(strEdgeId));
        Edge edge = checkEdgeId(edgeId, Operation.READ);

        EntityViewId entityViewId = new EntityViewId(toUUID(strEntityViewId));
        EntityView entityView = checkEntityViewId(entityViewId, Operation.READ);

        return tbEntityViewService.unassignEntityViewFromEdge(getTenantId(), entityView.getCustomerId(), entityView,
                edge, getCurrentUser());
    }

    /**
     * 分页列出已分配到指定 Edge 的实体视图，并按当前用户 READ 权限过滤。
     */
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/edge/{edgeId}/entityViews", params = {"pageSize", "page"}, method = RequestMethod.GET)
    @ResponseBody
    public PageData<EntityView> getEdgeEntityViews(
            @PathVariable(EDGE_ID) String strEdgeId,
            @RequestParam int pageSize,
            @RequestParam int page,
            @RequestParam(required = false) String type,
            @RequestParam(required = false) String textSearch,
            @RequestParam(required = false) String sortProperty,
            @RequestParam(required = false) String sortOrder,
            @RequestParam(required = false) Long startTime,
            @RequestParam(required = false) Long endTime) throws ThingsboardException {
        checkParameter(EDGE_ID, strEdgeId);
        TenantId tenantId = getCurrentUser().getTenantId();
        EdgeId edgeId = new EdgeId(toUUID(strEdgeId));
        checkEdgeId(edgeId, Operation.READ);
        TimePageLink pageLink = createTimePageLink(pageSize, page, textSearch, sortProperty, sortOrder, startTime, endTime);
        PageData<EntityView> nonFilteredResult;
        if (type != null && type.trim().length() > 0) {
            nonFilteredResult = entityViewService.findEntityViewsByTenantIdAndEdgeIdAndType(tenantId, edgeId, type, pageLink);
        } else {
            nonFilteredResult = entityViewService.findEntityViewsByTenantIdAndEdgeId(tenantId, edgeId, pageLink);
        }
        List<EntityView> filteredEntityViews = nonFilteredResult.getData().stream().filter(entityView -> {
            try {
                accessControlService.checkPermission(getCurrentUser(), Resource.ENTITY_VIEW, Operation.READ, entityView.getId(), entityView);
                return true;
            } catch (ThingsboardException e) {
                return false;
            }
        }).collect(Collectors.toList());
        PageData<EntityView> filteredResult = new PageData<>(filteredEntityViews,
                nonFilteredResult.getTotalPages(),
                nonFilteredResult.getTotalElements(),
                nonFilteredResult.hasNext());
        return checkNotNull(filteredResult);
    }
}
