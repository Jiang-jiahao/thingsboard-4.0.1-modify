package org.thingsboard.server.dao.entity;

import lombok.extern.slf4j.Slf4j;
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Lazy;
import org.thingsboard.common.util.DebugModeUtil;
import org.thingsboard.server.common.data.HasDebugSettings;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.debug.DebugSettings;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.relation.EntityRelation;
import org.thingsboard.server.dao.alarm.AlarmService;
import org.thingsboard.server.dao.cf.CalculatedFieldService;
import org.thingsboard.server.dao.entityview.EntityViewService;
import org.thingsboard.server.dao.exception.DataValidationException;
import org.thingsboard.server.dao.housekeeper.CleanUpService;
import org.thingsboard.server.dao.relation.RelationService;
import org.thingsboard.server.dao.tenant.TbTenantProfileCache;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractEntityService {

    public static final String INCORRECT_PAGE_LINK = "Incorrect page link ";

    @Autowired
    protected ApplicationEventPublisher eventPublisher;

    @Lazy
    @Autowired
    protected RelationService relationService;

    @Lazy
    @Autowired
    protected AlarmService alarmService;

    @Lazy
    @Autowired
    protected EntityViewService entityViewService;

    @Lazy
    @Autowired
    protected CalculatedFieldService calculatedFieldService;

    @Autowired
    @Lazy
    protected CleanUpService cleanUpService;

    @Autowired
    @Lazy
    private TbTenantProfileCache tbTenantProfileCache;

    @Value("${debug.settings.default_duration:15}")
    private int defaultDebugDurationMinutes;

    protected void createRelation(TenantId tenantId, EntityRelation relation) {
        log.debug("Creating relation: {}", relation);
        relationService.saveRelation(tenantId, relation);
    }

    protected void deleteRelation(TenantId tenantId, EntityRelation relation) {
        log.debug("Deleting relation: {}", relation);
        relationService.deleteRelation(tenantId, relation);
    }

    protected static Optional<ConstraintViolationException> extractConstraintViolationException(Exception t) {
        if (t instanceof ConstraintViolationException) {
            return Optional.of((ConstraintViolationException) t);
        } else if (t.getCause() instanceof ConstraintViolationException) {
            return Optional.of((ConstraintViolationException) (t.getCause()));
        } else {
            return Optional.empty();
        }
    }

    public static final void checkConstraintViolation(Exception t, String constraintName, String constraintMessage) {
        checkConstraintViolation(t, Collections.singletonMap(constraintName, constraintMessage));
    }

    public static final void checkConstraintViolation(Exception t, String constraintName1, String constraintMessage1, String constraintName2, String constraintMessage2) {
        checkConstraintViolation(t, Map.of(constraintName1, constraintMessage1, constraintName2, constraintMessage2));
    }

    public static final void checkConstraintViolation(Exception t, Map<String, String> constraints) {
        var exOpt = extractConstraintViolationException(t);
        if (exOpt.isPresent()) {
            var ex = exOpt.get();
            var constraintName = ex.getConstraintName();
            // PostgreSQL 的报错文本会跟随服务端 locale 本地化（中文库返回「重复键违反唯一约束"tb_user_email_key"」），
            // 而 Hibernate 是用英文正则从消息里提取约束名的，本地化后提取不到（返回 null），
            // 导致下面按名字精确匹配全部落空、原始 SQL 直接漏给前端。此时退回在根因异常文本里找。
            var fallbackMessage = StringUtils.isEmpty(constraintName) ? rootCauseMessage(t) : null;
            for (var constraintMessage : constraints.entrySet()) {
                var expectedName = constraintMessage.getKey();
                if (expectedName.equals(constraintName)
                        || (fallbackMessage != null && fallbackMessage.contains(expectedName))) {
                    throw new DataValidationException(constraintMessage.getValue());
                }
            }
        }
    }

    private static String rootCauseMessage(Throwable t) {
        var cause = t;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return StringUtils.defaultString(cause.getMessage(), "");
    }

    protected void updateDebugSettings(TenantId tenantId, HasDebugSettings entity, long now) {
        if (entity.getDebugSettings() != null) {
            entity.setDebugSettings(entity.getDebugSettings().copy(getMaxDebugAllUntil(tenantId, now)));
        } else if (entity.isDebugMode()) {
            entity.setDebugSettings(DebugSettings.failuresOrUntil(getMaxDebugAllUntil(tenantId, now)));
            entity.setDebugMode(false);
        }
    }

    private long getMaxDebugAllUntil(TenantId tenantId, long now) {
        return now + TimeUnit.MINUTES.toMillis(DebugModeUtil.getMaxDebugAllDuration(tbTenantProfileCache.get(tenantId).getDefaultProfileConfiguration().getMaxDebugModeDurationMinutes(), defaultDebugDurationMinutes));
    }
}
