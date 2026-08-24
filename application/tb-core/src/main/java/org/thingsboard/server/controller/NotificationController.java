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
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.thingsboard.rule.engine.api.NotificationCenter;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.NotificationId;
import org.thingsboard.server.common.data.id.NotificationRequestId;
import org.thingsboard.server.common.data.id.NotificationTargetId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.notification.Notification;
import org.thingsboard.server.common.data.notification.NotificationDeliveryMethod;
import org.thingsboard.server.common.data.notification.NotificationRequest;
import org.thingsboard.server.common.data.notification.NotificationRequestInfo;
import org.thingsboard.server.common.data.notification.NotificationRequestPreview;
import org.thingsboard.server.common.data.notification.settings.NotificationSettings;
import org.thingsboard.server.common.data.notification.settings.UserNotificationSettings;
import org.thingsboard.server.common.data.notification.targets.MicrosoftTeamsNotificationTargetConfig;
import org.thingsboard.server.common.data.notification.targets.NotificationRecipient;
import org.thingsboard.server.common.data.notification.targets.NotificationTarget;
import org.thingsboard.server.common.data.notification.targets.NotificationTargetType;
import org.thingsboard.server.common.data.notification.targets.platform.PlatformUsersNotificationTargetConfig;
import org.thingsboard.server.common.data.notification.targets.slack.SlackConversation;
import org.thingsboard.server.common.data.notification.targets.slack.SlackNotificationTargetConfig;
import org.thingsboard.server.common.data.notification.template.DeliveryMethodNotificationTemplate;
import org.thingsboard.server.common.data.notification.template.NotificationTemplate;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.page.SortOrder;
import org.thingsboard.server.config.annotations.ApiOperation;
import org.thingsboard.server.dao.notification.NotificationRequestService;
import org.thingsboard.server.dao.notification.NotificationService;
import org.thingsboard.server.dao.notification.NotificationSettingsService;
import org.thingsboard.server.dao.notification.NotificationTargetService;
import org.thingsboard.server.dao.notification.NotificationTemplateService;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.notification.NotificationProcessingContext;
import org.thingsboard.server.service.security.model.SecurityUser;
import org.thingsboard.server.service.security.permission.Operation;
import org.thingsboard.server.service.security.permission.Resource;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.thingsboard.server.controller.ControllerConstants.AVAILABLE_FOR_ANY_AUTHORIZED_USER;
import static org.thingsboard.server.controller.ControllerConstants.MARKDOWN_CODE_BLOCK_END;
import static org.thingsboard.server.controller.ControllerConstants.MARKDOWN_CODE_BLOCK_START;
import static org.thingsboard.server.controller.ControllerConstants.NEW_LINE;
import static org.thingsboard.server.controller.ControllerConstants.PAGE_DATA_PARAMETERS;
import static org.thingsboard.server.controller.ControllerConstants.PAGE_NUMBER_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.PAGE_SIZE_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.SORT_ORDER_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.SORT_PROPERTY_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH;
import static org.thingsboard.server.service.security.permission.Resource.NOTIFICATION;

/**
 * 通知中心 REST 入口：用户收件箱、手动发送请求、租户/系统投递配置、用户个人偏好。
 * <p>
 * 收件箱走 {@link NotificationService}；发送/已读/删除走 {@link NotificationCenter}；
 * 请求记录 {@link NotificationRequestService}；预览会用到模板/目标服务。
 * 仅在 {@link TbCoreComponent} 中生效。
 * <p>
 * <b>URL 前缀：</b>{@code /api}。路径如 {@code /notifications}、{@code /notification/request}、
 * {@code /notification/settings}。
 * <p>
 * <b>权限：</b>收件箱/已读/删除/个人设置对 SYS_ADMIN、TENANT_ADMIN、CUSTOMER_USER 开放；
 * 创建发送请求与租户级设置仅 SYS_ADMIN、TENANT_ADMIN。
 *
 * @see NotificationCenter
 * @see NotificationService
 * @see NotificationRequestService
 * @see NotificationSettingsService
 */
@RestController
@TbCoreComponent
@RequestMapping("/api")
@RequiredArgsConstructor
@Slf4j
public class NotificationController extends BaseController {

    private final NotificationService notificationService;
    private final NotificationRequestService notificationRequestService;
    private final NotificationTemplateService notificationTemplateService;
    private final NotificationTargetService notificationTargetService;
    private final NotificationCenter notificationCenter;
    private final NotificationSettingsService notificationSettingsService;

    /**
     * 分页列出当前用户的通知；可只看未读，并按 WEB / MOBILE_APP 投递方式过滤。
     */
    @ApiOperation(value = "Get notifications (getNotifications)",
            notes = "Returns the page of notifications for current user." + NEW_LINE +
                    PAGE_DATA_PARAMETERS +
                    AVAILABLE_FOR_ANY_AUTHORIZED_USER + NEW_LINE +
                    "**WebSocket API**:\n\n" +
                    "There are 2 types of subscriptions: one for unread notifications count, another for unread notifications themselves.\n\n" +
                    "The URI for opening WS session for notifications: `/api/ws/plugins/notifications`.\n\n" +
                    "Subscription command for unread notifications count:\n" +
                    "```\n{\n  \"unreadCountSubCmd\": {\n    \"cmdId\": 1234\n  }\n}\n```\n" +
                    "To subscribe for latest unread notifications:\n" +
                    "```\n{\n  \"unreadSubCmd\": {\n    \"cmdId\": 1234,\n    \"limit\": 10\n  }\n}\n```\n" +
                    "To unsubscribe from any subscription:\n" +
                    "```\n{\n  \"unsubCmd\": {\n    \"cmdId\": 1234\n  }\n}\n```\n" +
                    "To mark certain notifications as read, use following command:\n" +
                    "```\n{\n  \"markAsReadCmd\": {\n    \"cmdId\": 1234,\n    \"notifications\": [\n      \"6f860330-7fc2-11ed-b855-7dd3b7d2faa9\",\n      \"5b6dfee0-8d0d-11ed-b61f-35a57b03dade\"\n    ]\n  }\n}\n\n```\n" +
                    "To mark all notifications as read:\n" +
                    "```\n{\n  \"markAllAsReadCmd\": {\n    \"cmdId\": 1234\n  }\n}\n```\n" +
                    "\n\n" +
                    "Update structure for unread **notifications count subscription**:\n" +
                    "```\n{\n  \"cmdId\": 1234,\n  \"totalUnreadCount\": 55\n}\n```\n" +
                    "For **notifications subscription**:\n" +
                    "- full update of latest unread notifications:\n" +
                    "```\n{\n" +
                    "  \"cmdId\": 1234,\n" +
                    "  \"notifications\": [\n" +
                    "    {\n" +
                    "      \"id\": {\n" +
                    "        \"entityType\": \"NOTIFICATION\",\n" +
                    "        \"id\": \"6f860330-7fc2-11ed-b855-7dd3b7d2faa9\"\n" +
                    "      },\n" +
                    "      ...\n" +
                    "    }\n" +
                    "  ],\n" +
                    "  \"totalUnreadCount\": 1\n" +
                    "}\n```\n" +
                    "- when new notification arrives or shown notification is updated:\n" +
                    "```\n{\n" +
                    "  \"cmdId\": 1234,\n" +
                    "  \"update\": {\n" +
                    "    \"id\": {\n" +
                    "      \"entityType\": \"NOTIFICATION\",\n" +
                    "      \"id\": \"6f860330-7fc2-11ed-b855-7dd3b7d2faa9\"\n" +
                    "    },\n" +
                    "    # updated notification info, text, subject etc.\n" +
                    "    ...\n" +
                    "  },\n" +
                    "  \"totalUnreadCount\": 2\n" +
                    "}\n```\n" +
                    "- when unread notifications count changes:\n" +
                    "```\n{\n" +
                    "  \"cmdId\": 1234,\n" +
                    "  \"totalUnreadCount\": 5\n" +
                    "}\n```")
    @GetMapping("/notifications")
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    public PageData<Notification> getNotifications(@Parameter(description = PAGE_SIZE_DESCRIPTION, required = true)
                                                   @RequestParam int pageSize,
                                                   @Parameter(description = PAGE_NUMBER_DESCRIPTION, required = true)
                                                   @RequestParam int page,
                                                   @Parameter(description = "Case-insensitive 'substring' filter based on notification subject or text")
                                                   @RequestParam(required = false) String textSearch,
                                                   @Parameter(description = SORT_PROPERTY_DESCRIPTION)
                                                   @RequestParam(required = false) String sortProperty,
                                                   @Parameter(description = SORT_ORDER_DESCRIPTION)
                                                   @RequestParam(required = false) String sortOrder,
                                                   @Parameter(description = "To search for unread notifications only")
                                                   @RequestParam(defaultValue = "false") boolean unreadOnly,
                                                   @Parameter(description = "Delivery method", schema = @Schema(allowableValues = {"WEB", "MOBILE_APP"}))
                                                   @RequestParam(defaultValue = "WEB") NotificationDeliveryMethod deliveryMethod,
                                                   @AuthenticationPrincipal SecurityUser user) throws ThingsboardException {
        // no permissions
        PageLink pageLink = createPageLink(pageSize, page, textSearch, sortProperty, sortOrder);
        return notificationService.findNotificationsByRecipientIdAndReadStatus(user.getTenantId(), deliveryMethod, user.getId(), unreadOnly, pageLink);
    }

    /**
     * 当前用户指定投递方式下的未读条数。
     */
    @ApiOperation(value = "Get unread notifications count (getUnreadNotificationsCount)",
            notes = "Returns unread notifications count for chosen delivery method." +
                    AVAILABLE_FOR_ANY_AUTHORIZED_USER)
    @GetMapping("/notifications/unread/count")
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    public Integer getUnreadNotificationsCount(@Parameter(description = "Delivery method", schema = @Schema(allowableValues = {"WEB", "MOBILE_APP"}))
                                               @RequestParam(defaultValue = "MOBILE_APP") NotificationDeliveryMethod deliveryMethod,
                                               @AuthenticationPrincipal SecurityUser user) {
        return notificationService.countUnreadNotificationsByRecipientId(user.getTenantId(), deliveryMethod, user.getId());
    }

    /**
     * 将一条通知标为已读。
     */
    @ApiOperation(value = "Mark notification as read (markNotificationAsRead)",
            notes = "Marks notification as read by its id." +
                    AVAILABLE_FOR_ANY_AUTHORIZED_USER)
    @PutMapping("/notification/{id}/read")
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    public void markNotificationAsRead(@PathVariable UUID id,
                                       @AuthenticationPrincipal SecurityUser user) {
        // no permissions
        NotificationId notificationId = new NotificationId(id);
        notificationCenter.markNotificationAsRead(user.getTenantId(), user.getId(), notificationId);
    }

    /**
     * 将当前用户指定投递方式下的全部未读标为已读。
     */
    @ApiOperation(value = "Mark all notifications as read (markAllNotificationsAsRead)",
            notes = "Marks all unread notifications as read." +
                    AVAILABLE_FOR_ANY_AUTHORIZED_USER)
    @PutMapping("/notifications/read")
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    public void markAllNotificationsAsRead(@Parameter(description = "Delivery method", schema = @Schema(allowableValues = {"WEB", "MOBILE_APP"}))
                                           @RequestParam(defaultValue = "WEB") NotificationDeliveryMethod deliveryMethod,
                                           @AuthenticationPrincipal SecurityUser user) {
        // no permissions
        notificationCenter.markAllNotificationsAsRead(user.getTenantId(), deliveryMethod, user.getId());
    }

    /**
     * 删除当前用户的一条通知。
     */
    @ApiOperation(value = "Delete notification (deleteNotification)",
            notes = "Deletes notification by its id." +
                    AVAILABLE_FOR_ANY_AUTHORIZED_USER)
    @DeleteMapping("/notification/{id}")
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    public void deleteNotification(@PathVariable UUID id,
                                   @AuthenticationPrincipal SecurityUser user) {
        // no permissions
        NotificationId notificationId = new NotificationId(id);
        notificationCenter.deleteNotification(user.getTenantId(), user.getId(), notificationId);
    }

    /**
     * 提交一次手动发送请求（立即或延迟）。请求本身不可更新，只能取消/删除；发送异步进行。
     */
    @ApiOperation(value = "Create notification request (createNotificationRequest)",
            notes = "Processes notification request.\n" +
                    "Mandatory request properties are `targets` (list of targets ids to send notification to), " +
                    "and either `templateId` (existing notification template id) or `template` (to send notification without saving the template).\n" +
                    "Optionally, you can set `sendingDelayInSec` inside the `additionalConfig` field to schedule the notification." + NEW_LINE +
                    "For each enabled delivery method in the notification template, there must be a target in the `targets` list that supports this delivery method: " +
                    "if you chose `WEB`, `EMAIL` or `SMS` - there must be at least one target in `targets` of `PLATFORM_USERS` type.\n" +
                    "For `SLACK` delivery method - you need to chose at least one `SLACK` notification target." + NEW_LINE +
                    "Notification request object with `PROCESSING` status will be returned immediately, " +
                    "and the notification sending itself is done asynchronously. After all notifications are sent, " +
                    "the `status` of the request becomes `SENT`. Use `getNotificationRequestById` to see " +
                    "the notification request processing status and some sending stats. " + NEW_LINE +
                    "Here is an example of notification request to one target using saved template:\n" +
                    MARKDOWN_CODE_BLOCK_START +
                    "{\n" +
                    "  \"templateId\": {\n" +
                    "    \"entityType\": \"NOTIFICATION_TEMPLATE\",\n" +
                    "    \"id\": \"6dbc3670-e4dd-11ed-9401-dbcc5dff78be\"\n" +
                    "  },\n" +
                    "  \"targets\": [\n" +
                    "    \"320e3ed0-d785-11ed-a06c-21dd57dd88ca\"\n" +
                    "  ],\n" +
                    "  \"additionalConfig\": {\n" +
                    "    \"sendingDelayInSec\": 0\n" +
                    "  }\n" +
                    "}" +
                    MARKDOWN_CODE_BLOCK_END +
                    SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH
    )
    @PostMapping("/notification/request")
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    public NotificationRequest createNotificationRequest(@RequestBody @Valid NotificationRequest notificationRequest,
                                                         @AuthenticationPrincipal SecurityUser user) throws Exception {
        if (notificationRequest.getId() != null) {
            throw new IllegalArgumentException("Notification request cannot be updated. You may only cancel/delete it");
        }
        notificationRequest.setTenantId(user.getTenantId());
        checkEntity(notificationRequest.getId(), notificationRequest, NOTIFICATION);
        List<NotificationTargetId> targets = notificationRequest.getTargets().stream()
                .map(NotificationTargetId::new)
                .toList();
        for (NotificationTargetId targetId : targets) {
            checkNotificationTargetId(targetId, Operation.READ);
        }

        notificationRequest.setOriginatorEntityId(user.getId());
        notificationRequest.setInfo(null);
        notificationRequest.setRuleId(null);
        notificationRequest.setStatus(null);
        notificationRequest.setStats(null);

        return doSaveAndLog(EntityType.NOTIFICATION_REQUEST, notificationRequest, (tenantId, request) -> notificationCenter.processNotificationRequest(tenantId, request, null));
    }

    /**
     * 预览发送效果：接收人样例、各目标人数、各投递方式渲染后的模板。
     */
    @ApiOperation(value = "Get notification request preview (getNotificationRequestPreview)",
            notes = "Returns preview for notification request." + NEW_LINE +
                    "`processedTemplates` shows how the notifications for each delivery method will look like " +
                    "for the first recipient of the corresponding notification target." +
                    SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH)
    @PostMapping("/notification/request/preview")
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    public NotificationRequestPreview getNotificationRequestPreview(@RequestBody @Valid NotificationRequest request,
                                                                    @Parameter(description = "Amount of the recipients to show in preview")
                                                                    @RequestParam(defaultValue = "20") int recipientsPreviewSize,
                                                                    @AuthenticationPrincipal SecurityUser user) throws ThingsboardException {
        // PE: generic permission
        NotificationTemplate template;
        if (request.getTemplateId() != null) {
            template = checkEntityId(request.getTemplateId(), notificationTemplateService::findNotificationTemplateById, Operation.READ);
        } else {
            template = request.getTemplate();
        }
        if (template == null) {
            throw new IllegalArgumentException("Template is missing");
        }
        request.setOriginatorEntityId(user.getId());
        List<NotificationTarget> targets = request.getTargets().stream()
                .map(NotificationTargetId::new)
                .map(targetId -> {
                    NotificationTarget target = notificationTargetService.findNotificationTargetById(user.getTenantId(), targetId);
                    if (target == null) {
                        throw new IllegalArgumentException("Notification target for id " + targetId + " not found");
                    }
                    return target;
                })
                .sorted(Comparator.comparing(target -> target.getConfiguration().getType()))
                .toList();

        NotificationRequestPreview preview = new NotificationRequestPreview();

        Set<String> recipientsPreview = new LinkedHashSet<>();
        Map<String, Integer> recipientsCountByTarget = new LinkedHashMap<>();
        Map<NotificationTargetType, NotificationRecipient> firstRecipient = new HashMap<>();
        for (NotificationTarget target : targets) {
            checkEntity(getCurrentUser(), target, Operation.READ);

            int recipientsCount;
            List<NotificationRecipient> recipientsPart;
            NotificationTargetType targetType = target.getConfiguration().getType();
            switch (targetType) {
                case PLATFORM_USERS: {
                    PageData<User> recipients = notificationTargetService.findRecipientsForNotificationTargetConfig(user.getTenantId(),
                            (PlatformUsersNotificationTargetConfig) target.getConfiguration(), new PageLink(recipientsPreviewSize, 0, null,
                                    SortOrder.BY_CREATED_TIME_DESC));
                    recipientsCount = (int) recipients.getTotalElements();
                    recipientsPart = recipients.getData().stream().map(r -> (NotificationRecipient) r).collect(Collectors.toList());
                    break;
                }
                case SLACK: {
                    recipientsCount = 1;
                    recipientsPart = List.of(((SlackNotificationTargetConfig) target.getConfiguration()).getConversation());
                    break;
                }
                case MICROSOFT_TEAMS: {
                    recipientsCount = 1;
                    recipientsPart = List.of(((MicrosoftTeamsNotificationTargetConfig) target.getConfiguration()));
                    break;
                }
                default:
                    throw new IllegalArgumentException("Target type " + targetType + " not supported");
            }
            firstRecipient.putIfAbsent(targetType, !recipientsPart.isEmpty() ? recipientsPart.get(0) : null);
            for (NotificationRecipient recipient : recipientsPart) {
                if (recipientsPreview.size() < recipientsPreviewSize) {
                    String title = recipient.getTitle();
                    if (recipient instanceof SlackConversation) {
                        title = ((SlackConversation) recipient).getPointer() + title;
                    } else if (recipient instanceof User) {
                        if (!title.equals(recipient.getEmail())) {
                            title += " (" + recipient.getEmail() + ")";
                        }
                    }
                    recipientsPreview.add(title);
                } else {
                    break;
                }
            }
            recipientsCountByTarget.put(target.getName(), recipientsCount);
        }
        preview.setRecipientsPreview(recipientsPreview);
        preview.setRecipientsCountByTarget(recipientsCountByTarget);
        preview.setTotalRecipientsCount(recipientsCountByTarget.values().stream().mapToInt(Integer::intValue).sum());

        Set<NotificationDeliveryMethod> deliveryMethods = template.getConfiguration().getDeliveryMethodsTemplates().entrySet()
                .stream().filter(entry -> entry.getValue().isEnabled()).map(Map.Entry::getKey).collect(Collectors.toSet());
        NotificationProcessingContext ctx = NotificationProcessingContext.builder()
                .tenantId(user.getTenantId())
                .request(request)
                .deliveryMethods(deliveryMethods)
                .template(template)
                .settings(null)
                .build();
        Map<NotificationDeliveryMethod, DeliveryMethodNotificationTemplate> processedTemplates = ctx.getDeliveryMethods().stream()
                .collect(Collectors.toMap(m -> m, deliveryMethod -> {
                    NotificationTargetType targetType = NotificationTargetType.forDeliveryMethod(deliveryMethod);
                    return ctx.getProcessedTemplate(deliveryMethod, firstRecipient.get(targetType));
                }));
        preview.setProcessedTemplates(processedTemplates);

        return preview;
    }

    /**
     * 按 Id 读取发送请求信息（状态、统计等）。
     */
    @ApiOperation(value = "Get notification request by id (getNotificationRequestById)",
            notes = "Fetches notification request info by request id." +
                    SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH)
    @GetMapping("/notification/request/{id}")
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    public NotificationRequestInfo getNotificationRequestById(@PathVariable UUID id) throws ThingsboardException {
        NotificationRequestId notificationRequestId = new NotificationRequestId(id);
        return checkEntityId(notificationRequestId, notificationRequestService::findNotificationRequestInfoById, Operation.READ);
    }

    /**
     * 分页列出本租户用户提交的发送请求。
     */
    @ApiOperation(value = "Get notification requests (getNotificationRequests)",
            notes = "Returns the page of notification requests submitted by users of this tenant or sysadmins." + NEW_LINE +
                    PAGE_DATA_PARAMETERS +
                    SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH)
    @GetMapping("/notification/requests")
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    public PageData<NotificationRequestInfo> getNotificationRequests(@Parameter(description = PAGE_SIZE_DESCRIPTION, required = true)
                                                                     @RequestParam int pageSize,
                                                                     @Parameter(description = PAGE_NUMBER_DESCRIPTION, required = true)
                                                                     @RequestParam int page,
                                                                     @Parameter(description = "Case-insensitive 'substring' filed based on the used template name")
                                                                     @RequestParam(required = false) String textSearch,
                                                                     @Parameter(description = SORT_PROPERTY_DESCRIPTION)
                                                                     @RequestParam(required = false) String sortProperty,
                                                                     @Parameter(description = SORT_ORDER_DESCRIPTION)
                                                                     @RequestParam(required = false) String sortOrder,
                                                                     @AuthenticationPrincipal SecurityUser user) throws ThingsboardException {
        // PE: generic permission
        PageLink pageLink = createPageLink(pageSize, page, textSearch, sortProperty, sortOrder);
        return notificationRequestService.findNotificationRequestsInfosByTenantIdAndOriginatorType(user.getTenantId(), EntityType.USER, pageLink);
    }

    /**
     * 删除发送请求：已 SENT 则删掉已发通知，SCHEDULED 则取消。
     */
    @ApiOperation(value = "Delete notification request (deleteNotificationRequest)",
            notes = "Deletes notification request by its id." + NEW_LINE +
                    "If the request has status `SENT` - all sent notifications for this request will be deleted. " +
                    "If it is `SCHEDULED`, the request will be cancelled." +
                    SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH)
    @DeleteMapping("/notification/request/{id}")
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    public void deleteNotificationRequest(@PathVariable UUID id) throws Exception {
        NotificationRequestId notificationRequestId = new NotificationRequestId(id);
        NotificationRequest notificationRequest = checkEntityId(notificationRequestId, notificationRequestService::findNotificationRequestById, Operation.DELETE);
        doDeleteAndLog(EntityType.NOTIFICATION_REQUEST, notificationRequest, notificationCenter::deleteNotificationRequest);
    }


    /**
     * 保存租户或系统级通知设置（各投递方式配置，如 Slack bot token）。
     */
    @ApiOperation(value = "Save notification settings (saveNotificationSettings)",
            notes = "Saves notification settings for this tenant or sysadmin.\n" +
                    "`deliveryMethodsConfigs` of the settings must be specified." + NEW_LINE +
                    "Here is an example of the notification settings with Slack configuration:\n" +
                    MARKDOWN_CODE_BLOCK_START +
                    "{\n" +
                    "  \"deliveryMethodsConfigs\": {\n" +
                    "    \"SLACK\": {\n" +
                    "      \"method\": \"SLACK\",\n" +
                    "      \"botToken\": \"xoxb-....\"\n" +
                    "    }\n" +
                    "  }\n" +
                    "}" +
                    MARKDOWN_CODE_BLOCK_END +
                    SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH)
    @PostMapping("/notification/settings")
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    public NotificationSettings saveNotificationSettings(@RequestBody @Valid NotificationSettings notificationSettings,
                                                         @AuthenticationPrincipal SecurityUser user) throws ThingsboardException {
        accessControlService.checkPermission(user, Resource.ADMIN_SETTINGS, Operation.WRITE);
        TenantId tenantId = user.isSystemAdmin() ? TenantId.SYS_TENANT_ID : user.getTenantId();
        notificationSettingsService.saveNotificationSettings(tenantId, notificationSettings);
        return notificationSettings;
    }

    /**
     * 读取租户或系统级通知设置。
     */
    @ApiOperation(value = "Get notification settings (getNotificationSettings)",
            notes = "Retrieves notification settings for this tenant or sysadmin." +
                    SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH)
    @GetMapping("/notification/settings")
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    public NotificationSettings getNotificationSettings(@AuthenticationPrincipal SecurityUser user) throws ThingsboardException {
        accessControlService.checkPermission(user, Resource.ADMIN_SETTINGS, Operation.READ);
        TenantId tenantId = user.isSystemAdmin() ? TenantId.SYS_TENANT_ID : user.getTenantId();
        return notificationSettingsService.findNotificationSettings(tenantId);
    }

    /**
     * 返回当前租户已正确配置、可用于发送的投递方式列表。
     */
    @ApiOperation(value = "Get available delivery methods (getAvailableDeliveryMethods)",
            notes = "Returns the list of delivery methods that are properly configured and are allowed to be used for sending notifications." +
                    SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH)
    @GetMapping("/notification/deliveryMethods")
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    public List<NotificationDeliveryMethod> getAvailableDeliveryMethods(@AuthenticationPrincipal SecurityUser user) throws ThingsboardException {
        return notificationCenter.getAvailableDeliveryMethods(user.getTenantId());
    }


    /**
     * 保存当前用户的通知偏好（各类型/渠道开关）。
     */
    @PostMapping("/notification/settings/user")
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    public UserNotificationSettings saveUserNotificationSettings(@RequestBody @Valid UserNotificationSettings settings,
                                                                 @AuthenticationPrincipal SecurityUser user) {
        return notificationSettingsService.saveUserNotificationSettings(user.getTenantId(), user.getId(), settings);
    }

    /**
     * 读取当前用户的通知偏好。
     */
    @GetMapping("/notification/settings/user")
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    public UserNotificationSettings getUserNotificationSettings(@AuthenticationPrincipal SecurityUser user) {
        return notificationSettingsService.getUserNotificationSettings(user.getTenantId(), user.getId(), true);
    }

}
