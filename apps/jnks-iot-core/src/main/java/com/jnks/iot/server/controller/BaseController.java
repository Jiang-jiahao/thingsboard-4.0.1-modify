package com.jnks.iot.server.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.ListenableFuture;
import jakarta.mail.MessagingException;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.ConstraintViolation;
import lombok.Getter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.hibernate.exception.ConstraintViolationException;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.async.AsyncRequestTimeoutException;
import org.springframework.web.context.request.async.DeferredResult;
import com.jnks.iot.common.util.DonAsynchron;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.cluster.JnksIotClusterService;
import com.jnks.iot.server.common.data.Customer;
import com.jnks.iot.server.common.data.Dashboard;
import com.jnks.iot.server.common.data.DashboardInfo;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceInfo;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.EntityView;
import com.jnks.iot.server.common.data.EntityViewInfo;
import com.jnks.iot.server.common.data.HasName;
import com.jnks.iot.server.common.data.HasTenantId;
import com.jnks.iot.server.common.data.HomeDashboardInfo;
import com.jnks.iot.server.common.data.OtaPackage;
import com.jnks.iot.server.common.data.OtaPackageInfo;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.JnksIotResource;
import com.jnks.iot.server.common.data.JnksIotResourceInfo;
import com.jnks.iot.server.common.data.Tenant;
import com.jnks.iot.server.common.data.TenantInfo;
import com.jnks.iot.server.common.data.TenantProfile;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.alarm.Alarm;
import com.jnks.iot.server.common.data.alarm.AlarmComment;
import com.jnks.iot.server.common.data.alarm.AlarmInfo;
import com.jnks.iot.server.common.data.asset.Asset;
import com.jnks.iot.server.common.data.asset.AssetInfo;
import com.jnks.iot.server.common.data.asset.AssetProfile;
import com.jnks.iot.server.common.data.audit.ActionType;
import com.jnks.iot.server.common.data.cf.CalculatedField;
import com.jnks.iot.server.common.data.domain.Domain;
import com.jnks.iot.server.common.data.exception.EntityVersionMismatchException;
import com.jnks.iot.server.common.data.exception.JnksIotErrorCode;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.AlarmCommentId;
import com.jnks.iot.server.common.data.id.AlarmId;
import com.jnks.iot.server.common.data.id.AssetId;
import com.jnks.iot.server.common.data.id.AssetProfileId;
import com.jnks.iot.server.common.data.id.CalculatedFieldId;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.DashboardId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.common.data.id.DomainId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.EntityIdFactory;
import com.jnks.iot.server.common.data.id.EntityViewId;
import com.jnks.iot.server.common.data.id.HasId;
import com.jnks.iot.server.common.data.id.MobileAppBundleId;
import com.jnks.iot.server.common.data.id.MobileAppId;
import com.jnks.iot.server.common.data.id.NotificationTargetId;
import com.jnks.iot.server.common.data.id.OAuth2ClientId;
import com.jnks.iot.server.common.data.id.OtaPackageId;
import com.jnks.iot.server.common.data.id.QueueId;
import com.jnks.iot.server.common.data.id.RpcId;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.id.RuleNodeId;
import com.jnks.iot.server.common.data.id.JnksIotResourceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.TenantProfileId;
import com.jnks.iot.server.common.data.id.UUIDBased;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.id.WidgetTypeId;
import com.jnks.iot.server.common.data.id.WidgetsBundleId;
import com.jnks.iot.server.common.data.mobile.app.MobileApp;
import com.jnks.iot.server.common.data.mobile.bundle.MobileAppBundle;
import com.jnks.iot.server.common.data.notification.targets.NotificationTarget;
import com.jnks.iot.server.common.data.oauth2.OAuth2Client;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.page.SortOrder;
import com.jnks.iot.server.common.data.page.TimePageLink;
import com.jnks.iot.server.common.data.plugin.ComponentDescriptor;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.query.EntityDataSortOrder;
import com.jnks.iot.server.common.data.query.EntityKey;
import com.jnks.iot.server.common.data.queue.Queue;
import com.jnks.iot.server.common.data.rpc.Rpc;
import com.jnks.iot.server.common.data.rule.RuleChain;
import com.jnks.iot.server.common.data.rule.RuleChainType;
import com.jnks.iot.server.common.data.rule.RuleNode;
import com.jnks.iot.server.common.data.security.UserCredentials;
import com.jnks.iot.server.common.data.util.ThrowingBiFunction;
import com.jnks.iot.server.common.data.widget.WidgetTypeDetails;
import com.jnks.iot.server.common.data.widget.WidgetTypeInfo;
import com.jnks.iot.server.common.data.widget.WidgetsBundle;
import com.jnks.iot.server.dao.alarm.AlarmCommentService;
import com.jnks.iot.server.dao.asset.AssetProfileService;
import com.jnks.iot.server.dao.asset.AssetService;
import com.jnks.iot.server.dao.attributes.AttributesService;
import com.jnks.iot.server.dao.audit.AuditLogService;
import com.jnks.iot.server.dao.cf.CalculatedFieldService;
import com.jnks.iot.server.dao.customer.CustomerService;
import com.jnks.iot.server.dao.dashboard.DashboardService;
import com.jnks.iot.server.dao.device.ClaimDevicesService;
import com.jnks.iot.server.dao.device.DeviceCredentialsService;
import com.jnks.iot.server.dao.device.DeviceProfileService;
import com.jnks.iot.server.dao.device.DeviceService;
import com.jnks.iot.server.dao.domain.DomainService;
import com.jnks.iot.server.dao.entityview.EntityViewService;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.exception.IncorrectParameterException;
import com.jnks.iot.server.dao.mobile.MobileAppBundleService;
import com.jnks.iot.server.dao.mobile.MobileAppService;
import com.jnks.iot.server.dao.model.ModelConstants;
import com.jnks.iot.server.dao.notification.NotificationTargetService;
import com.jnks.iot.server.dao.oauth2.OAuth2ClientService;
import com.jnks.iot.server.dao.oauth2.OAuth2ConfigTemplateService;
import com.jnks.iot.server.dao.ota.OtaPackageService;
import com.jnks.iot.server.dao.queue.QueueService;
import com.jnks.iot.server.dao.relation.RelationService;
import com.jnks.iot.server.dao.resource.ResourceService;
import com.jnks.iot.server.dao.rpc.RpcService;
import com.jnks.iot.server.dao.rule.RuleChainService;
import com.jnks.iot.server.dao.service.ConstraintValidator;
import com.jnks.iot.server.dao.service.Validator;
import com.jnks.iot.server.dao.tenant.JnksIotTenantProfileCache;
import com.jnks.iot.server.dao.tenant.TenantProfileService;
import com.jnks.iot.server.dao.tenant.TenantService;
import com.jnks.iot.server.dao.user.UserService;
import com.jnks.iot.server.dao.widget.WidgetTypeService;
import com.jnks.iot.server.dao.widget.WidgetsBundleService;
import com.jnks.iot.server.exception.JnksIotErrorResponseHandler;
import com.jnks.iot.server.queue.discovery.PartitionService;
import com.jnks.iot.server.queue.discovery.JnksIotServiceInfoProvider;
import com.jnks.iot.server.queue.provider.JnksIotQueueProducerProvider;
import com.jnks.iot.server.service.action.EntityActionService;
import com.jnks.iot.server.service.component.ComponentDiscoveryService;
import com.jnks.iot.server.service.entitiy.JnksIotLogEntityActionService;
import com.jnks.iot.server.service.entitiy.user.JnksIotUserSettingsService;
import com.jnks.iot.server.service.ota.OtaPackageStateService;
import com.jnks.iot.server.service.profile.JnksIotAssetProfileCache;
import com.jnks.iot.server.service.profile.JnksIotDeviceProfileCache;
import com.jnks.iot.server.service.security.model.SecurityUser;
import com.jnks.iot.server.service.security.permission.AccessControlService;
import com.jnks.iot.server.service.security.permission.Operation;
import com.jnks.iot.server.service.security.permission.Resource;
import com.jnks.iot.server.service.state.DeviceStateService;
import com.jnks.iot.server.service.sync.ie.exporting.ExportableEntitiesService;
import com.jnks.iot.server.service.sync.vc.EntitiesVersionControlService;
import com.jnks.iot.server.service.telemetry.AlarmSubscriptionService;
import com.jnks.iot.server.service.telemetry.TelemetrySubscriptionService;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import static com.jnks.iot.server.common.data.StringUtils.isNotEmpty;
import static com.jnks.iot.server.common.data.query.EntityKeyType.ENTITY_FIELD;
import static com.jnks.iot.server.controller.ControllerConstants.DEFAULT_DASHBOARD;
import static com.jnks.iot.server.controller.ControllerConstants.HOME_DASHBOARD;
import static com.jnks.iot.server.controller.UserController.YOU_DON_T_HAVE_PERMISSION_TO_PERFORM_THIS_OPERATION;
import static com.jnks.iot.server.dao.service.Validator.validateId;

/**
 * jnks-iot-core REST Controller 的公共基类：当前用户、租户、实体权限校验、分页参数、异常映射。
 * <p>
 * 仅在 jnks-iot-core 模块 中生效。本身不暴露业务 URL；子类经 {@link #getCurrentUser()}
 * 取登录用户，经 {@code checkXxxId} / {@link #checkEntityId} 做存在性 + ACL，
 * 再调用各自的 {@code JnksIotXxxService}。
 * <p>
 * 权限判定委托 {@link AccessControlService}（按 {@link Resource} + {@link Operation}）。
 * DAO 服务通过字段注入，供校验方法和少数直接查询复用。
 *
 * @see AccessControlService
 * @see com.jnks.iot.server.service.security.model.SecurityUser
 */
public abstract class BaseController {

    protected static final String DASHBOARD_ID = "dashboardId";

    protected static final String HOME_DASHBOARD_ID = "homeDashboardId";
    protected static final String HOME_DASHBOARD_HIDE_TOOLBAR = "homeDashboardHideToolbar";

    protected final Logger log = org.slf4j.LoggerFactory.getLogger(getClass());

    /*Swagger UI description*/

    @Autowired
    private JnksIotErrorResponseHandler errorResponseHandler;

    /** ACL 入口：按 Resource + Operation 校验当前用户能否操作某实体。 */
    @Autowired
    protected AccessControlService accessControlService;

    @Autowired
    protected TenantService tenantService;

    @Autowired
    protected TenantProfileService tenantProfileService;

    @Autowired
    protected CustomerService customerService;

    @Autowired
    protected UserService userService;

    @Autowired
    protected JnksIotUserSettingsService userSettingsService;

    @Autowired
    protected DeviceService deviceService;

    @Autowired
    protected DeviceProfileService deviceProfileService;

    @Autowired
    protected AssetService assetService;

    @Autowired
    protected AssetProfileService assetProfileService;

    @Autowired
    protected AlarmSubscriptionService alarmService;

    @Autowired
    protected AlarmCommentService alarmCommentService;

    @Autowired
    protected DeviceCredentialsService deviceCredentialsService;

    @Autowired
    protected WidgetsBundleService widgetsBundleService;

    @Autowired
    protected WidgetTypeService widgetTypeService;

    @Autowired
    protected DashboardService dashboardService;

    @Autowired
    protected OAuth2ClientService oAuth2ClientService;

    @Autowired
    protected DomainService domainService;

    @Autowired
    protected MobileAppService mobileAppService;

    @Autowired
    protected MobileAppBundleService mobileAppBundleService;

    @Autowired
    protected OAuth2ConfigTemplateService oAuth2ConfigTemplateService;

    @Autowired
    protected ComponentDiscoveryService componentDescriptorService;

    @Autowired
    protected RuleChainService ruleChainService;

    @Autowired
    protected JnksIotClusterService jnksIotClusterService;

    @Autowired
    protected RelationService relationService;

    @Autowired
    protected AuditLogService auditLogService;

    @Autowired
    protected DeviceStateService deviceStateService;

    @Autowired
    protected EntityViewService entityViewService;

    @Autowired
    protected TelemetrySubscriptionService tsSubService;

    @Autowired
    protected AttributesService attributesService;

    @Autowired
    protected ClaimDevicesService claimDevicesService;

    @Autowired
    protected PartitionService partitionService;

    @Autowired
    protected ResourceService resourceService;

    @Autowired
    protected OtaPackageService otaPackageService;

    @Autowired
    protected OtaPackageStateService otaPackageStateService;

    @Autowired
    protected RpcService rpcService;

    @Autowired
    protected JnksIotQueueProducerProvider producerProvider;

    @Autowired
    protected JnksIotTenantProfileCache tenantProfileCache;

    @Autowired
    protected JnksIotDeviceProfileCache deviceProfileCache;

    @Autowired
    protected JnksIotAssetProfileCache assetProfileCache;

    @Autowired
    protected JnksIotLogEntityActionService logEntityActionService;

    @Autowired
    protected EntityActionService entityActionService;

    @Autowired
    protected QueueService queueService;

    @Autowired
    protected EntitiesVersionControlService vcService;

    @Autowired
    protected ExportableEntitiesService entitiesService;

    @Autowired
    protected JnksIotServiceInfoProvider serviceInfoProvider;

    @Autowired
    protected NotificationTargetService notificationTargetService;

    @Autowired
    protected CalculatedFieldService calculatedFieldService;

    @Value("${server.log_controller_error_stack_trace}")
    @Getter
    private boolean logControllerErrorStackTrace;

    /**
     * 未声明的异常统一转成 {@link JnksIotException} 再写 HTTP 错误响应。
     */
    @ExceptionHandler(Exception.class)
    public void handleControllerException(Exception e, HttpServletResponse response) {
        JnksIotException jnksIotException = handleException(e);
        if (jnksIotException.getErrorCode() == JnksIotErrorCode.GENERAL && jnksIotException.getCause() instanceof Exception
                && StringUtils.equals(jnksIotException.getCause().getMessage(), jnksIotException.getMessage())) {
            e = (Exception) jnksIotException.getCause();
        } else {
            e = jnksIotException;
        }
        errorResponseHandler.handle(e, response);
    }

    /**
     * 直接把 {@link JnksIotException} 写成 HTTP 错误响应。
     */
    @ExceptionHandler(JnksIotException.class)
    public void handleJnksIotException(JnksIotException ex, HttpServletResponse response) {
        errorResponseHandler.handle(ex, response);
    }

    /**
     * @deprecated Exceptions that are not of {@link JnksIotException} type
     * are now caught and mapped to {@link JnksIotException} by
     * {@link ExceptionHandler} {@link BaseController#handleControllerException(Exception, HttpServletResponse)}
     * which basically acts like the following boilerplate:
     * {@code
     *  try {
     *      someExceptionThrowingMethod();
     *  } catch (Exception e) {
     *      throw handleException(e);
     *  }
     * }
     * */
    @Deprecated
    JnksIotException handleException(Exception exception) {
        return handleException(exception, true);
    }

    private JnksIotException handleException(Exception exception, boolean logException) {
        if (logException && logControllerErrorStackTrace) {
            try {
                SecurityUser user = getCurrentUser();
                log.error("[{}][{}] Error", user.getTenantId(), user.getId(), exception);
            } catch (Exception e) {
                log.error("Error", exception);
            }
        }

        Throwable cause = exception.getCause();
        if (exception instanceof JnksIotException) {
            return (JnksIotException) exception;
        } else if (exception instanceof IllegalArgumentException || exception instanceof IncorrectParameterException
                || exception instanceof DataValidationException || cause instanceof IncorrectParameterException) {
            return new JnksIotException(exception.getMessage(), JnksIotErrorCode.BAD_REQUEST_PARAMS);
        } else if (exception instanceof MessagingException) {
            return new JnksIotException("Unable to send mail", JnksIotErrorCode.GENERAL);
        } else if (exception instanceof AsyncRequestTimeoutException) {
            return new JnksIotException("Request timeout", JnksIotErrorCode.GENERAL);
        } else if (exception instanceof DataAccessException) {
            if (!logControllerErrorStackTrace) { // not to log the error twice
                log.warn("Database error: {} - {}", exception.getClass().getSimpleName(), ExceptionUtils.getRootCauseMessage(exception));
            }
            if (cause instanceof ConstraintViolationException) {
                return new JnksIotException(ExceptionUtils.getRootCause(exception).getMessage(), JnksIotErrorCode.BAD_REQUEST_PARAMS);
            } else {
                return new JnksIotException("Database error", JnksIotErrorCode.GENERAL);
            }
        } else if (exception instanceof EntityVersionMismatchException) {
            return new JnksIotException(exception.getMessage(), exception, JnksIotErrorCode.VERSION_CONFLICT);
        }
        return new JnksIotException(exception.getMessage(), exception, JnksIotErrorCode.GENERAL);
    }

    /**
     * 处理 {@code @Valid} 参数校验失败，拼错误信息后按 400 返回。
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public void handleValidationError(MethodArgumentNotValidException validationError, HttpServletResponse response) {
        List<ConstraintViolation<Object>> constraintsViolations = validationError.getFieldErrors().stream()
                .map(fieldError -> {
                    try {
                        return (ConstraintViolation<Object>) fieldError.unwrap(ConstraintViolation.class);
                    } catch (Exception e) {
                        log.warn("FieldError source is not of type ConstraintViolation");
                        return null; // should not happen
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        String errorMessage = "Validation error: " + ConstraintValidator.getErrorMessage(constraintsViolations);
        JnksIotException jnksIotException = new JnksIotException(errorMessage, JnksIotErrorCode.BAD_REQUEST_PARAMS);
        handleControllerException(jnksIotException, response);
    }

    /** 引用为 null 则抛 ITEM_NOT_FOUND。 */
    <T> T checkNotNull(T reference) throws JnksIotException {
        return checkNotNull(reference, "Requested item wasn't found!");
    }

    <T> T checkNotNull(T reference, String notFoundMessage) throws JnksIotException {
        if (reference == null) {
            throw new JnksIotException(notFoundMessage, JnksIotErrorCode.ITEM_NOT_FOUND);
        }
        return reference;
    }

    <T> T checkNotNull(Optional<T> reference) throws JnksIotException {
        return checkNotNull(reference, "Requested item wasn't found!");
    }

    <T> T checkNotNull(Optional<T> reference, String notFoundMessage) throws JnksIotException {
        if (reference.isPresent()) {
            return reference.get();
        } else {
            throw new JnksIotException(notFoundMessage, JnksIotErrorCode.ITEM_NOT_FOUND);
        }
    }

    /** 路径/查询参数为空则抛 BAD_REQUEST。 */
    void checkParameter(String name, String param) throws JnksIotException {
        if (StringUtils.isEmpty(param)) {
            throw new JnksIotException("Parameter '" + name + "' can't be empty!", JnksIotErrorCode.BAD_REQUEST_PARAMS);
        }
    }

    void checkArrayParameter(String name, String[] params) throws JnksIotException {
        if (params == null || params.length == 0) {
            throw new JnksIotException("Parameter '" + name + "' can't be empty!", JnksIotErrorCode.BAD_REQUEST_PARAMS);
        } else {
            for (String param : params) {
                checkParameter(name, param);
            }
        }
    }

    /** 把字符串转成枚举；非法值抛 BAD_REQUEST。 */
    protected <T> T checkEnumParameter(String name, String param, Function<String, T> valueOf) throws JnksIotException {
        try {
            return valueOf.apply(param.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new JnksIotException(name + " \"" + param + "\" is not supported!", JnksIotErrorCode.BAD_REQUEST_PARAMS);
        }
    }

    UUID toUUID(String id) throws JnksIotException {
        try {
            return UUID.fromString(id);
        } catch (IllegalArgumentException e) {
            throw handleException(e, false);
        }
    }

    PageLink createPageLink(int pageSize, int page, String textSearch, String sortProperty, String sortOrder) throws JnksIotException {
        if (StringUtils.isNotEmpty(sortProperty)) {
            if (!Validator.isValidProperty(sortProperty)) {
                throw new IllegalArgumentException("Invalid sort property");
            }
            SortOrder.Direction direction = SortOrder.Direction.ASC;
            if (StringUtils.isNotEmpty(sortOrder)) {
                try {
                    direction = SortOrder.Direction.valueOf(sortOrder.toUpperCase());
                } catch (IllegalArgumentException e) {
                    throw new JnksIotException("Unsupported sort order '" + sortOrder + "'! Only 'ASC' or 'DESC' types are allowed.", JnksIotErrorCode.BAD_REQUEST_PARAMS);
                }
            }
            SortOrder sort = new SortOrder(sortProperty, direction);
            return new PageLink(pageSize, page, textSearch, sort);
        } else {
            return new PageLink(pageSize, page, textSearch);
        }
    }

    TimePageLink createTimePageLink(int pageSize, int page, String textSearch,
                                    String sortProperty, String sortOrder, Long startTime, Long endTime) throws JnksIotException {
        PageLink pageLink = this.createPageLink(pageSize, page, textSearch, sortProperty, sortOrder);
        return new TimePageLink(pageLink, startTime, endTime);
    }

    /**
     * 从 SecurityContext 取当前登录用户；未认证则抛 AUTHENTICATION。
     */
    protected SecurityUser getCurrentUser() throws JnksIotException {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null && authentication.getPrincipal() instanceof SecurityUser) {
            return (SecurityUser) authentication.getPrincipal();
        } else {
            throw new JnksIotException("You aren't authorized to perform this operation!", JnksIotErrorCode.AUTHENTICATION);
        }
    }

    /** 加载租户并校验当前用户对该租户有指定操作权限。 */
    Tenant checkTenantId(TenantId tenantId, Operation operation) throws JnksIotException {
        return checkEntityId(tenantId, (t, i) -> tenantService.findTenantById(tenantId), operation);
    }

    TenantInfo checkTenantInfoId(TenantId tenantId, Operation operation) throws JnksIotException {
        return checkEntityId(tenantId, (t, i) -> tenantService.findTenantInfoById(tenantId), operation);
    }

    /** 加载租户档案；权限按 TENANT_PROFILE 资源校验（不按单条实体）。 */
    TenantProfile checkTenantProfileId(TenantProfileId tenantProfileId, Operation operation) throws JnksIotException {
        try {
            validateId(tenantProfileId, id -> "Incorrect tenantProfileId " + id);
            TenantProfile tenantProfile = tenantProfileService.findTenantProfileById(getTenantId(), tenantProfileId);
            checkNotNull(tenantProfile, "Tenant profile with id [" + tenantProfileId + "] is not found");
            accessControlService.checkPermission(getCurrentUser(), Resource.TENANT_PROFILE, operation);
            return tenantProfile;
        } catch (Exception e) {
            throw handleException(e, false);
        }
    }

    /** 当前登录用户所属租户 ID。 */
    protected TenantId getTenantId() throws JnksIotException {
        return getCurrentUser().getTenantId();
    }

    /** 加载客户并校验权限。 */
    Customer checkCustomerId(CustomerId customerId, Operation operation) throws JnksIotException {
        return checkEntityId(customerId, customerService::findCustomerById, operation);
    }

    /** 加载用户并校验权限。 */
    User checkUserId(UserId userId, Operation operation) throws JnksIotException {
        return checkEntityId(userId, userService::findUserById, operation);
    }

    /**
     * 保存前权限：无 ID 走 CREATE，有 ID 走 WRITE（先加载实体再 ACL）。
     */
    protected <I extends EntityId, T extends HasTenantId> void checkEntity(I entityId, T entity, Resource resource) throws JnksIotException {
        if (entityId == null) {
            accessControlService.checkPermission(getCurrentUser(), resource, Operation.CREATE, null, entity);
        } else {
            checkEntityId(entityId, Operation.WRITE);
        }
    }

    /**
     * 按实体类型分发到对应 {@code checkXxxId}，完成存在性与 ACL。
     */
    protected void checkEntityId(EntityId entityId, Operation operation) throws JnksIotException {
        try {
            if (entityId == null) {
                throw new JnksIotException("Parameter entityId can't be empty!", JnksIotErrorCode.BAD_REQUEST_PARAMS);
            }
            validateId(entityId.getId(), id -> "Incorrect entityId " + id);
            switch (entityId.getEntityType()) {
                case ALARM:
                    checkAlarmId(new AlarmId(entityId.getId()), operation);
                    return;
                case DEVICE:
                    checkDeviceId(new DeviceId(entityId.getId()), operation);
                    return;
                case DEVICE_PROFILE:
                    checkDeviceProfileId(new DeviceProfileId(entityId.getId()), operation);
                    return;
                case CUSTOMER:
                    checkCustomerId(new CustomerId(entityId.getId()), operation);
                    return;
                case TENANT:
                    checkTenantId(TenantId.fromUUID(entityId.getId()), operation);
                    return;
                case TENANT_PROFILE:
                    checkTenantProfileId(new TenantProfileId(entityId.getId()), operation);
                    return;
                case RULE_CHAIN:
                    checkRuleChain(new RuleChainId(entityId.getId()), operation);
                    return;
                case RULE_NODE:
                    checkRuleNode(new RuleNodeId(entityId.getId()), operation);
                    return;
                case ASSET:
                    checkAssetId(new AssetId(entityId.getId()), operation);
                    return;
                case ASSET_PROFILE:
                    checkAssetProfileId(new AssetProfileId(entityId.getId()), operation);
                    return;
                case DASHBOARD:
                    checkDashboardId(new DashboardId(entityId.getId()), operation);
                    return;
                case USER:
                    checkUserId(new UserId(entityId.getId()), operation);
                    return;
                case ENTITY_VIEW:
                    checkEntityViewId(new EntityViewId(entityId.getId()), operation);
                    return;
                case WIDGETS_BUNDLE:
                    checkWidgetsBundleId(new WidgetsBundleId(entityId.getId()), operation);
                    return;
                case WIDGET_TYPE:
                    checkWidgetTypeId(new WidgetTypeId(entityId.getId()), operation);
                    return;
                case JNKS_IOT_RESOURCE:
                    checkResourceInfoId(new JnksIotResourceId(entityId.getId()), operation);
                    return;
                case OTA_PACKAGE:
                    checkOtaPackageId(new OtaPackageId(entityId.getId()), operation);
                    return;
                case QUEUE:
                    checkQueueId(new QueueId(entityId.getId()), operation);
                    return;
                case OAUTH2_CLIENT:
                    checkOauth2ClientId(new OAuth2ClientId(entityId.getId()), operation);
                    return;
                case DOMAIN:
                    checkDomainId(new DomainId(entityId.getId()), operation);
                    return;
                case MOBILE_APP:
                    checkMobileAppId(new MobileAppId(entityId.getId()), operation);
                    return;
                case MOBILE_APP_BUNDLE:
                    checkMobileAppBundleId(new MobileAppBundleId(entityId.getId()), operation);
                    return;
                case CALCULATED_FIELD:
                    checkCalculatedFieldId(new CalculatedFieldId(entityId.getId()), operation);
                    return;
                default:
                    checkEntityId(entityId, entitiesService::findEntityByTenantIdAndId, operation);
            }
        } catch (Exception e) {
            throw handleException(e, false);
        }
    }

    /**
     * 用 findingFunction 按当前租户加载实体，再交给 {@link #checkEntity(SecurityUser, HasId, Operation)}。
     */
    protected <E extends HasId<I> & HasTenantId, I extends EntityId> E checkEntityId(I entityId, ThrowingBiFunction<TenantId, I, E> findingFunction, Operation operation) throws JnksIotException {
        try {
            validateId((UUIDBased) entityId, "Invalid entity id");
            SecurityUser user = getCurrentUser();
            E entity = findingFunction.apply(user.getTenantId(), entityId);
            checkNotNull(entity, entityId.getEntityType().getNormalName() + " with id [" + entityId + "] is not found");
            return checkEntity(user, entity, operation);
        } catch (Exception e) {
            throw handleException(e, false);
        }
    }

    /**
     * 对已加载实体做 ACL：{@link AccessControlService#checkPermission}。
     */
    protected <E extends HasId<I> & HasTenantId, I extends EntityId> E checkEntity(SecurityUser user, E entity, Operation operation) throws JnksIotException {
        checkNotNull(entity, "Entity not found");
        accessControlService.checkPermission(user, Resource.of(entity.getId().getEntityType()), operation, entity.getId(), entity);
        return entity;
    }

    /** 加载设备并校验权限。 */
    Device checkDeviceId(DeviceId deviceId, Operation operation) throws JnksIotException {
        return checkEntityId(deviceId, deviceService::findDeviceById, operation);
    }

    /** 加载设备 Info 并校验权限。 */
    DeviceInfo checkDeviceInfoId(DeviceId deviceId, Operation operation) throws JnksIotException {
        return checkEntityId(deviceId, deviceService::findDeviceInfoById, operation);
    }

    /** 加载设备档案并校验权限。 */
    DeviceProfile checkDeviceProfileId(DeviceProfileId deviceProfileId, Operation operation) throws JnksIotException {
        return checkEntityId(deviceProfileId, deviceProfileService::findDeviceProfileById, operation);
    }

    /** 加载实体视图并校验权限。 */
    protected EntityView checkEntityViewId(EntityViewId entityViewId, Operation operation) throws JnksIotException {
        return checkEntityId(entityViewId, entityViewService::findEntityViewById, operation);
    }

    EntityViewInfo checkEntityViewInfoId(EntityViewId entityViewId, Operation operation) throws JnksIotException {
        return checkEntityId(entityViewId, entityViewService::findEntityViewInfoById, operation);
    }

    /** 加载资产并校验权限。 */
    Asset checkAssetId(AssetId assetId, Operation operation) throws JnksIotException {
        return checkEntityId(assetId, assetService::findAssetById, operation);
    }

    /** 加载资产 Info 并校验权限。 */
    AssetInfo checkAssetInfoId(AssetId assetId, Operation operation) throws JnksIotException {
        return checkEntityId(assetId, assetService::findAssetInfoById, operation);
    }

    /** 加载资产档案并校验权限。 */
    AssetProfile checkAssetProfileId(AssetProfileId assetProfileId, Operation operation) throws JnksIotException {
        return checkEntityId(assetProfileId, assetProfileService::findAssetProfileById, operation);
    }

    /** 加载告警并校验权限（originator 归属）。 */
    Alarm checkAlarmId(AlarmId alarmId, Operation operation) throws JnksIotException {
        return checkEntityId(alarmId, alarmService::findAlarmById, operation);
    }

    /** 加载告警 Info 并校验权限。 */
    AlarmInfo checkAlarmInfoId(AlarmId alarmId, Operation operation) throws JnksIotException {
        return checkEntityId(alarmId, alarmService::findAlarmInfoById, operation);
    }

    /**
     * 加载告警评论，并校验其 alarmId 与路径上的告警一致。
     */
    AlarmComment checkAlarmCommentId(AlarmCommentId alarmCommentId, AlarmId alarmId) throws JnksIotException {
        try {
            validateId(alarmCommentId, id -> "Incorrect alarmCommentId " + id);
            AlarmComment alarmComment = alarmCommentService.findAlarmCommentByIdAsync(getCurrentUser().getTenantId(), alarmCommentId).get();
            checkNotNull(alarmComment, "Alarm comment with id [" + alarmCommentId + "] is not found");
            if (!alarmId.equals(alarmComment.getAlarmId())) {
                throw new JnksIotException("Alarm id does not match with comment alarm id", JnksIotErrorCode.BAD_REQUEST_PARAMS);
            }
            return alarmComment;
        } catch (Exception e) {
            throw handleException(e, false);
        }
    }

    WidgetsBundle checkWidgetsBundleId(WidgetsBundleId widgetsBundleId, Operation operation) throws JnksIotException {
        return checkEntityId(widgetsBundleId, widgetsBundleService::findWidgetsBundleById, operation);
    }

    WidgetTypeDetails checkWidgetTypeId(WidgetTypeId widgetTypeId, Operation operation) throws JnksIotException {
        return checkEntityId(widgetTypeId, widgetTypeService::findWidgetTypeDetailsById, operation);
    }

    WidgetTypeInfo checkWidgetTypeInfoId(WidgetTypeId widgetTypeId, Operation operation) throws JnksIotException {
        return checkEntityId(widgetTypeId, widgetTypeService::findWidgetTypeInfoById, operation);
    }

    /** 加载仪表盘并校验权限。 */
    Dashboard checkDashboardId(DashboardId dashboardId, Operation operation) throws JnksIotException {
        return checkEntityId(dashboardId, dashboardService::findDashboardById, operation);
    }

    /** 加载仪表盘 Info 并校验权限。 */
    DashboardInfo checkDashboardInfoId(DashboardId dashboardId, Operation operation) throws JnksIotException {
        return checkEntityId(dashboardId, dashboardService::findDashboardInfoById, operation);
    }

    /** 按规则节点类名加载组件描述符。 */
    ComponentDescriptor checkComponentDescriptorByClazz(String clazz) throws JnksIotException {
        try {
            log.debug("[{}] Lookup component descriptor", clazz);
            return checkNotNull(componentDescriptorService.getComponent(clazz));
        } catch (Exception e) {
            throw handleException(e, false);
        }
    }

    List<ComponentDescriptor> checkComponentDescriptorsByType(ComponentType type, RuleChainType ruleChainType) throws JnksIotException {
        try {
            log.debug("[{}] Lookup component descriptors", type);
            return componentDescriptorService.getComponents(type, ruleChainType);
        } catch (Exception e) {
            throw handleException(e, false);
        }
    }

    List<ComponentDescriptor> checkComponentDescriptorsByTypes(Set<ComponentType> types, RuleChainType ruleChainType) throws JnksIotException {
        try {
            log.debug("[{}] Lookup component descriptors", types);
            return componentDescriptorService.getComponents(types, ruleChainType);
        } catch (Exception e) {
            throw handleException(e, false);
        }
    }

    /** 加载规则链并校验权限。 */
    protected RuleChain checkRuleChain(RuleChainId ruleChainId, Operation operation) throws JnksIotException {
        return checkEntityId(ruleChainId, ruleChainService::findRuleChainById, operation);
    }

    /**
     * 加载规则节点，权限按所属规则链校验。
     */
    protected RuleNode checkRuleNode(RuleNodeId ruleNodeId, Operation operation) throws JnksIotException {
        validateId(ruleNodeId, id -> "Incorrect ruleNodeId " + id);
        RuleNode ruleNode = ruleChainService.findRuleNodeById(getTenantId(), ruleNodeId);
        checkNotNull(ruleNode, "Rule node with id [" + ruleNodeId + "] is not found");
        checkRuleChain(ruleNode.getRuleChainId(), operation);
        return ruleNode;
    }

    JnksIotResource checkResourceId(JnksIotResourceId resourceId, Operation operation) throws JnksIotException {
        return checkEntityId(resourceId, resourceService::findResourceById, operation);
    }

    JnksIotResourceInfo checkResourceInfoId(JnksIotResourceId resourceId, Operation operation) throws JnksIotException {
        return checkEntityId(resourceId, resourceService::findResourceInfoById, operation);
    }

    OtaPackage checkOtaPackageId(OtaPackageId otaPackageId, Operation operation) throws JnksIotException {
        return checkEntityId(otaPackageId, otaPackageService::findOtaPackageById, operation);
    }

    OtaPackageInfo checkOtaPackageInfoId(OtaPackageId otaPackageId, Operation operation) throws JnksIotException {
        return checkEntityId(otaPackageId, otaPackageService::findOtaPackageInfoById, operation);
    }

    Rpc checkRpcId(RpcId rpcId, Operation operation) throws JnksIotException {
        return checkEntityId(rpcId, rpcService::findById, operation);
    }

    /**
     * 加载队列并校验权限。系统队列在租户开启独立规则引擎时对普通租户拒绝。
     */
    protected Queue checkQueueId(QueueId queueId, Operation operation) throws JnksIotException {
        Queue queue = checkEntityId(queueId, queueService::findQueueById, operation);
        TenantId tenantId = getTenantId();
        if (queue.getTenantId().isNullUid() && !tenantId.isNullUid()) {
            TenantProfile tenantProfile = tenantProfileCache.get(tenantId);
            if (tenantProfile.isIsolatedJnksIotRuleEngine()) {
                throw new JnksIotException(YOU_DON_T_HAVE_PERMISSION_TO_PERFORM_THIS_OPERATION,
                        JnksIotErrorCode.PERMISSION_DENIED);
            }
        }
        return queue;
    }

    OAuth2Client checkOauth2ClientId(OAuth2ClientId oAuth2ClientId, Operation operation) throws JnksIotException {
        return checkEntityId(oAuth2ClientId, oAuth2ClientService::findOAuth2ClientById, operation);
    }

    Domain checkDomainId(DomainId domainId, Operation operation) throws JnksIotException {
        return checkEntityId(domainId, domainService::findDomainById, operation);
    }

    MobileApp checkMobileAppId(MobileAppId mobileAppId, Operation operation) throws JnksIotException {
        return checkEntityId(mobileAppId, mobileAppService::findMobileAppById, operation);
    }

    MobileAppBundle checkMobileAppBundleId(MobileAppBundleId mobileAppBundleId, Operation operation) throws JnksIotException {
        return checkEntityId(mobileAppBundleId, mobileAppBundleService::findMobileAppBundleById, operation);
    }

    NotificationTarget checkNotificationTargetId(NotificationTargetId notificationTargetId, Operation operation) throws JnksIotException {
        return checkEntityId(notificationTargetId, notificationTargetService::findNotificationTargetById, operation);
    }

    protected <I extends EntityId> I emptyId(EntityType entityType) {
        return (I) EntityIdFactory.getByTypeAndUuid(entityType, ModelConstants.NULL_UUID);
    }

    public static Exception toException(Throwable error) {
        return error != null ? (Exception.class.isInstance(error) ? (Exception) error : new Exception(error)) : null;
    }

    protected <E extends HasName & HasId<? extends EntityId>> void logEntityAction(SecurityUser user, EntityType entityType, E savedEntity, ActionType actionType) {
        logEntityAction(user, entityType, null, savedEntity, actionType, null);
    }

    protected <E extends HasName & HasId<? extends EntityId>> void logEntityAction(SecurityUser user, EntityType entityType, E entity, E savedEntity, ActionType actionType, Exception e) {
        EntityId entityId = savedEntity != null ? savedEntity.getId() : emptyId(entityType);
        if (!user.isSystemAdmin()) {
            entityActionService.logEntityAction(user, entityId, savedEntity != null ? savedEntity : entity,
                    user.getCustomerId(), actionType, e);
        }
    }

    /**
     * 保存实体并写审计：成功记 ADDED/UPDATED，失败也记一条带异常的日志。
     */
    protected <E extends HasName & HasId<? extends EntityId>> E doSaveAndLog(EntityType entityType, E entity, BiFunction<TenantId, E, E> savingFunction) throws Exception {
        ActionType actionType = entity.getId() == null ? ActionType.ADDED : ActionType.UPDATED;
        SecurityUser user = getCurrentUser();
        try {
            E savedEntity = savingFunction.apply(user.getTenantId(), entity);
            logEntityAction(user, entityType, savedEntity, actionType);
            return savedEntity;
        } catch (Exception e) {
            logEntityAction(user, entityType, entity, null, actionType, e);
            throw e;
        }
    }

    /**
     * 删除实体并写审计（含失败路径）。
     */
    protected <E extends HasName & HasId<I>, I extends EntityId> void doDeleteAndLog(EntityType entityType, E entity, BiConsumer<TenantId, I> deleteFunction) throws Exception {
        SecurityUser user = getCurrentUser();
        try {
            deleteFunction.accept(user.getTenantId(), entity.getId());
            logEntityAction(user, entityType, entity, ActionType.DELETED);
        } catch (Exception e) {
            logEntityAction(user, entityType, entity, entity, ActionType.DELETED, e);
            throw e;
        }
    }

    /**
     * 补全用户 additionalInfo（凭据是否启用/已激活/上次登录），并校验其中仪表盘 ID。
     */
    protected void checkUserInfo(User user) throws JnksIotException {
        ObjectNode info;
        if (user.getAdditionalInfo() instanceof ObjectNode additionalInfo) {
            info = additionalInfo;
            checkDashboardInfo(info);
        } else {
            info = JacksonUtil.newObjectNode();
            user.setAdditionalInfo(info);
        }

        UserCredentials userCredentials = userService.findUserCredentialsByUserId(user.getTenantId(), user.getId());
        info.put("userCredentialsEnabled", userCredentials.isEnabled());
        info.put("userActivated", userCredentials.getActivateToken() == null);
        info.put("lastLoginTs", userCredentials.getLastLoginTs());
    }

    /**
     * 若 additionalInfo 里的仪表盘 ID 已不存在则从 JSON 中删掉该字段。
     */
    protected void checkDashboardInfo(JsonNode additionalInfo) throws JnksIotException {
        checkDashboardInfo(additionalInfo, DEFAULT_DASHBOARD);
        checkDashboardInfo(additionalInfo, HOME_DASHBOARD);
    }

    protected void checkDashboardInfo(JsonNode node, String dashboardField) throws JnksIotException {
        if (node instanceof ObjectNode additionalInfo) {
            DashboardId dashboardId = Optional.ofNullable(additionalInfo.get(dashboardField))
                    .filter(JsonNode::isTextual).map(JsonNode::asText)
                    .map(id -> {
                        try {
                            return new DashboardId(UUID.fromString(id));
                        } catch (IllegalArgumentException e) {
                            return null;
                        }
                    }).orElse(null);

            if (dashboardId != null && !dashboardService.existsById(getTenantId(), dashboardId)) {
                additionalInfo.remove(dashboardField);
            }
        }
    }

    /** 加载计算字段并校验权限。 */
    protected CalculatedField checkCalculatedFieldId(CalculatedFieldId calculatedFieldId, Operation operation) throws JnksIotException {
        return checkEntityId(calculatedFieldId, calculatedFieldService::findById, operation);
    }

    /**
     * 解析首页仪表盘：先用户 additionalInfo，客户用户再看客户，最后看租户。
     */
    protected HomeDashboardInfo getHomeDashboardInfo(SecurityUser securityUser, JsonNode additionalInfo) {
        HomeDashboardInfo homeDashboardInfo = extractHomeDashboardInfoFromAdditionalInfo(additionalInfo);
        if (homeDashboardInfo == null) {
            if (securityUser.isCustomerUser()) {
                Customer customer = customerService.findCustomerById(securityUser.getTenantId(), securityUser.getCustomerId());
                homeDashboardInfo = extractHomeDashboardInfoFromAdditionalInfo(customer.getAdditionalInfo());
            }
            if (homeDashboardInfo == null) {
                Tenant tenant = tenantService.findTenantById(securityUser.getTenantId());
                homeDashboardInfo = extractHomeDashboardInfoFromAdditionalInfo(tenant.getAdditionalInfo());
            }
        }
        return homeDashboardInfo;
    }

    private HomeDashboardInfo extractHomeDashboardInfoFromAdditionalInfo(JsonNode additionalInfo) {
        try {
            if (additionalInfo != null && additionalInfo.has(HOME_DASHBOARD_ID) && !additionalInfo.get(HOME_DASHBOARD_ID).isNull()) {
                String strDashboardId = additionalInfo.get(HOME_DASHBOARD_ID).asText();
                DashboardId dashboardId = new DashboardId(toUUID(strDashboardId));
                checkDashboardId(dashboardId, Operation.READ);
                boolean hideDashboardToolbar = true;
                if (additionalInfo.has(HOME_DASHBOARD_HIDE_TOOLBAR)) {
                    hideDashboardToolbar = additionalInfo.get(HOME_DASHBOARD_HIDE_TOOLBAR).asBoolean();
                }
                return new HomeDashboardInfo(dashboardId, hideDashboardToolbar);
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    protected MediaType parseMediaType(String contentType) {
        try {
            return MediaType.parseMediaType(contentType);
        } catch (Exception e) {
            return MediaType.APPLICATION_OCTET_STREAM;
        }
    }

    /** 把 ListenableFuture 接到 Spring {@link DeferredResult}（用 MVC 默认异步超时）。 */
    protected <T> DeferredResult<T> wrapFuture(ListenableFuture<T> future) {
        DeferredResult<T> deferredResult = new DeferredResult<>(); // Timeout of spring.mvc.async.request-timeout is used
        DonAsynchron.withCallback(future, deferredResult::setResult, deferredResult::setErrorResult);
        return deferredResult;
    }

    protected <T> DeferredResult<T> wrapFuture(ListenableFuture<T> future, long timeoutMs) {
        DeferredResult<T> deferredResult = new DeferredResult<>(timeoutMs);
        DonAsynchron.withCallback(future, deferredResult::setResult, deferredResult::setErrorResult);
        return deferredResult;
    }

    protected EntityDataSortOrder createEntityDataSortOrder(String sortProperty, String sortOrder) {
        if (isNotEmpty(sortProperty)) {
            EntityDataSortOrder entityDataSortOrder = new EntityDataSortOrder();
            entityDataSortOrder.setKey(new EntityKey(ENTITY_FIELD, sortProperty));
            if (isNotEmpty(sortOrder)) {
                entityDataSortOrder.setDirection(EntityDataSortOrder.Direction.valueOf(sortOrder));
            }
            return entityDataSortOrder;
        } else {
            return null;
        }
    }

    protected void compressResponseWithGzipIFAccepted(String acceptEncodingHeader, HttpServletResponse response, byte[] content) throws IOException {
        if (StringUtils.isNotEmpty(acceptEncodingHeader) && acceptEncodingHeader.contains("gzip")) {
            response.setHeader(HttpHeaders.CONTENT_ENCODING, "gzip");
            response.setCharacterEncoding(StandardCharsets.UTF_8.displayName());
            try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(response.getOutputStream())) {
                gzipOutputStream.write(content);
                gzipOutputStream.finish();
            }
        } else {
            try (ServletOutputStream outputStream = response.getOutputStream()) {
                outputStream.write(content);
                outputStream.flush();
            }
        }
    }

    protected <T> ResponseEntity<T> response(HttpStatus status) {
        return ResponseEntity.status(status).build();
    }

    protected <T> ResponseEntity<T> redirectTo(String location) {
        URI uri;
        try {
            uri = URI.create(location);
        } catch (IllegalArgumentException e) {
            log.error("Failed to create URI from '{}'", location, e);
            throw e;
        }
        return ResponseEntity.status(HttpStatus.SEE_OTHER)
                .location(uri)
                .build();
    }

    /**
     * 校验一组 OAuth2 客户端 ID 均存在且当前用户可读。
     */
    protected List<OAuth2ClientId> getOAuth2ClientIds(UUID[] ids) throws JnksIotException {
        if (ids == null) {
            return Collections.emptyList();
        }
        List<OAuth2ClientId> oAuth2ClientIds = new ArrayList<>();
        for (UUID id : ids) {
            OAuth2ClientId oauth2ClientId = new OAuth2ClientId(id);
            checkOauth2ClientId(oauth2ClientId, Operation.READ);
            oAuth2ClientIds.add(oauth2ClientId);
        }
        return oAuth2ClientIds;
    }

}
