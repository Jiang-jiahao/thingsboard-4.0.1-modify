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

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.QueueId;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.queue.Queue;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.config.annotations.ApiOperation;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.entitiy.queue.TbQueueService;
import org.thingsboard.server.service.security.permission.Operation;
import org.thingsboard.server.service.security.permission.Resource;

import java.util.UUID;

import static org.thingsboard.server.controller.ControllerConstants.PAGE_DATA_PARAMETERS;
import static org.thingsboard.server.controller.ControllerConstants.PAGE_NUMBER_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.PAGE_SIZE_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.QUEUE_ID_PARAM_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.QUEUE_NAME_PARAM_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.QUEUE_QUEUE_TEXT_SEARCH_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.QUEUE_SERVICE_TYPE_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.SORT_ORDER_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.SORT_PROPERTY_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.SYSTEM_AUTHORITY_PARAGRAPH;
import static org.thingsboard.server.controller.ControllerConstants.SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH;
import static org.thingsboard.server.controller.ControllerConstants.UUID_WIKI_LINK;

/**
 * 消息队列 REST 入口。
 * <p>
 * <b>职责：</b>管理平台注册的队列（目前仅 {@code TB-RULE-ENGINE} 类型有实现：分页、按 id/名称查询、创建更新、删除）。
 * 队列名在系统管理员范围内唯一。
 * <p>
 * <b>URL：</b>{@code /api/queues}
 * <p>
 * <b>权限：</b>写/删仅 {@code SYS_ADMIN}；查询 {@code SYS_ADMIN} 或 {@code TENANT_ADMIN}。
 * 写/删会校验资源 {@code QUEUE}。
 * <p>
 * <b>下游：</b>{@link TbQueueService}、{@code queueService}（{@link BaseController}）
 */
@RestController
@TbCoreComponent
@RequestMapping("/api")
@RequiredArgsConstructor
public class QueueController extends BaseController {

    private final TbQueueService tbQueueService;

    /**
     * 按服务类型分页查询队列。目前仅 {@code TB-RULE-ENGINE} 返回数据，其余类型为空页。
     * <p>
     * 权限：{@code SYS_ADMIN} 或 {@code TENANT_ADMIN}。下游 {@code queueService}。
     */
    @ApiOperation(value = "Get Queues (getTenantQueuesByServiceType)",
            notes = "Returns a page of queues registered in the platform. " +
                    PAGE_DATA_PARAMETERS + SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    @RequestMapping(value = "/queues", params = {"serviceType", "pageSize", "page"}, method = RequestMethod.GET)
    @ResponseBody
    public PageData<Queue> getTenantQueuesByServiceType(@Parameter(description = QUEUE_SERVICE_TYPE_DESCRIPTION, schema = @Schema(allowableValues = {"TB-RULE-ENGINE", "TB-CORE", "TB-TRANSPORT", "JS-EXECUTOR"}, requiredMode = Schema.RequiredMode.REQUIRED))
                                                        @RequestParam String serviceType,
                                                        @Parameter(description = PAGE_SIZE_DESCRIPTION, required = true)
                                                        @RequestParam int pageSize,
                                                        @Parameter(description = PAGE_NUMBER_DESCRIPTION, required = true)
                                                        @RequestParam int page,
                                                        @Parameter(description = QUEUE_QUEUE_TEXT_SEARCH_DESCRIPTION)
                                                        @RequestParam(required = false) String textSearch,
                                                        @Parameter(description = SORT_PROPERTY_DESCRIPTION, schema = @Schema(allowableValues = {"createdTime", "name", "topic"}))
                                                        @RequestParam(required = false) String sortProperty,
                                                        @Parameter(description = SORT_ORDER_DESCRIPTION, schema = @Schema(allowableValues = {"ASC", "DESC"}))
                                                        @RequestParam(required = false) String sortOrder) throws ThingsboardException {
        checkParameter("serviceType", serviceType);
        PageLink pageLink = createPageLink(pageSize, page, textSearch, sortProperty, sortOrder);
        ServiceType type = ServiceType.of(serviceType);
        switch (type) {
            case TB_RULE_ENGINE:
                return queueService.findQueuesByTenantId(getTenantId(), pageLink);
            default:
                return new PageData<>();
        }
    }

    /**
     * 按 id 查询队列，并校验当前用户对该队列的 READ 权限。
     * <p>
     * 权限：{@code SYS_ADMIN} 或 {@code TENANT_ADMIN}。下游 {@code queueService}。
     */
    @ApiOperation(value = "Get Queue (getQueueById)",
            notes = "Fetch the Queue object based on the provided Queue Id. " + SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    @RequestMapping(value = "/queues/{queueId}", method = RequestMethod.GET)
    @ResponseBody
    public Queue getQueueById(@Parameter(description = QUEUE_ID_PARAM_DESCRIPTION)
                              @PathVariable("queueId") String queueIdStr) throws ThingsboardException {
        checkParameter("queueId", queueIdStr);
        QueueId queueId = new QueueId(UUID.fromString(queueIdStr));
        checkQueueId(queueId, Operation.READ);
        return checkNotNull(queueService.findQueueById(getTenantId(), queueId));
    }

    /**
     * 按名称查询当前租户下的队列。
     * <p>
     * 权限：{@code SYS_ADMIN} 或 {@code TENANT_ADMIN}。下游 {@code queueService}。
     */
    @ApiOperation(value = "Get Queue (getQueueByName)",
            notes = "Fetch the Queue object based on the provided Queue name. " + SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    @RequestMapping(value = "/queues/name/{queueName}", method = RequestMethod.GET)
    @ResponseBody
    public Queue getQueueByName(@Parameter(description = QUEUE_NAME_PARAM_DESCRIPTION)
                                @PathVariable("queueName") String queueName) throws ThingsboardException {
        checkParameter("queueName", queueName);
        return checkNotNull(queueService.findQueueByTenantIdAndName(getTenantId(), queueName));
    }

    /**
     * 创建或更新队列。新建时平台生成 id；目前仅 {@code TB-RULE-ENGINE} 类型会真正保存，其余返回 null。
     * <p>
     * 权限：{@code SYS_ADMIN}；资源 {@code QUEUE}。下游 {@link TbQueueService#saveQueue}。
     */
    @ApiOperation(value = "Create Or Update Queue (saveQueue)",
            notes = "Create or update the Queue. When creating queue, platform generates Queue Id as " + UUID_WIKI_LINK +
                    "Specify existing Queue id to update the queue. " +
                    "Referencing non-existing Queue Id will cause 'Not Found' error." +
                    "\n\nQueue name is unique in the scope of sysadmin. " +
                    "Remove 'id', 'tenantId' from the request body example (below) to create new Queue entity. " +
                    SYSTEM_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN')")
    @RequestMapping(value = "/queues", params = {"serviceType"}, method = RequestMethod.POST)
    @ResponseBody
    public Queue saveQueue(@Parameter(description = "A JSON value representing the queue.")
                           @RequestBody Queue queue,
                           @Parameter(description = QUEUE_SERVICE_TYPE_DESCRIPTION, schema = @Schema(allowableValues = {"TB-RULE-ENGINE", "TB-CORE", "TB-TRANSPORT", "JS-EXECUTOR"}, requiredMode = Schema.RequiredMode.REQUIRED))
                           @RequestParam String serviceType) throws ThingsboardException {
        checkParameter("serviceType", serviceType);
        queue.setTenantId(getCurrentUser().getTenantId());

        checkEntity(queue.getId(), queue, Resource.QUEUE);

        ServiceType type = ServiceType.of(serviceType);
        switch (type) {
            case TB_RULE_ENGINE:
                queue.setTenantId(getTenantId());
                Queue savedQueue = tbQueueService.saveQueue(queue);
                checkNotNull(savedQueue);
                return savedQueue;
            default:
                return null;
        }
    }

    /**
     * 按 id 删除队列。
     * <p>
     * 权限：{@code SYS_ADMIN}；实体 DELETE。下游 {@link TbQueueService#deleteQueue}。
     */
    @ApiOperation(value = "Delete Queue (deleteQueue)", notes = "Deletes the Queue. " + SYSTEM_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN')")
    @RequestMapping(value = "/queues/{queueId}", method = RequestMethod.DELETE)
    @ResponseBody
    public void deleteQueue(@Parameter(description = QUEUE_ID_PARAM_DESCRIPTION)
                            @PathVariable("queueId") String queueIdStr) throws ThingsboardException {
        checkParameter("queueId", queueIdStr);
        QueueId queueId = new QueueId(toUUID(queueIdStr));
        checkQueueId(queueId, Operation.DELETE);
        tbQueueService.deleteQueue(getTenantId(), queueId);
    }
}
