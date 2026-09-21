package com.jnks.iot.server.service.entitiy.alarm;

import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.alarm.Alarm;
import com.jnks.iot.server.common.data.alarm.AlarmComment;
import com.jnks.iot.server.common.data.alarm.AlarmCommentType;
import com.jnks.iot.server.common.data.audit.ActionType;
import com.jnks.iot.server.common.data.exception.JnksIotErrorCode;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.dao.alarm.AlarmCommentService;
import com.jnks.iot.server.service.entitiy.AbstractJnksIotEntityService;

/**
 * {@link JnksIotAlarmCommentService} 的默认实现。
 * <p>
 * 由告警评论 Controller / {@link DefaultJnksIotAlarmService} 调用，委托 {@link AlarmCommentService} 落库。
 * 用户评论删除会改写成系统评论并写审计；系统评论不可删。
 *
 * @see JnksIotAlarmCommentService
 */
@Service
@AllArgsConstructor
public class DefaultJnksIotAlarmCommentService extends AbstractJnksIotEntityService implements JnksIotAlarmCommentService{

    @Autowired
    private AlarmCommentService alarmCommentService;

    /** 保存告警评论并写 ADDED/UPDATED_COMMENT 审计。 */
    @Override
    public AlarmComment saveAlarmComment(Alarm alarm, AlarmComment alarmComment, User user) throws JnksIotException {
        ActionType actionType = alarmComment.getId() == null ? ActionType.ADDED_COMMENT : ActionType.UPDATED_COMMENT;
        if (user != null) {
            alarmComment.setUserId(user.getId());
        }
        try {
            AlarmComment savedAlarmComment = checkNotNull(alarmCommentService.createOrUpdateAlarmComment(alarm.getTenantId(), alarmComment));
            logEntityActionService.logEntityAction(alarm.getTenantId(), alarm.getId(), alarm, alarm.getCustomerId(), actionType, user, savedAlarmComment);

            return savedAlarmComment;
        } catch (Exception e) {
            logEntityActionService.logEntityAction(alarm.getTenantId(), emptyId(EntityType.ALARM), alarm, actionType, user, e, alarmComment);
            throw e;
        }
    }

    /** 将用户评论改写为系统删除说明；系统评论则拒绝删除。 */
    @Override
    public void deleteAlarmComment(Alarm alarm, AlarmComment alarmComment, User user) throws JnksIotException {
        if (alarmComment.getType() == AlarmCommentType.OTHER) {
            alarmComment.setType(AlarmCommentType.SYSTEM);
            alarmComment.setUserId(null);
            alarmComment.setComment(JacksonUtil.newObjectNode().put("text",
                    String.format("User %s deleted his comment",
                            (user.getFirstName() == null || user.getLastName() == null) ? user.getName() : user.getFirstName() + " " + user.getLastName())));
            AlarmComment savedAlarmComment = checkNotNull(alarmCommentService.saveAlarmComment(alarm.getTenantId(), alarmComment));
            logEntityActionService.logEntityAction(alarm.getTenantId(), alarm.getId(), alarm, alarm.getCustomerId(), ActionType.DELETED_COMMENT, user, savedAlarmComment);
        } else {
            throw new JnksIotException("System comment could not be deleted", JnksIotErrorCode.BAD_REQUEST_PARAMS);
        }
    }
}
