package com.jnks.iot.server.dao.model.sql;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.MappedSuperclass;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.server.common.data.alarm.AlarmComment;
import com.jnks.iot.server.common.data.alarm.AlarmCommentType;
import com.jnks.iot.server.common.data.id.AlarmCommentId;
import com.jnks.iot.server.common.data.id.AlarmId;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.dao.model.BaseSqlEntity;
import com.jnks.iot.server.dao.model.ModelConstants;
import com.jnks.iot.server.dao.util.mapping.JsonConverter;

import java.util.UUID;

import static com.jnks.iot.server.dao.model.ModelConstants.ALARM_COMMENT_ALARM_ID;
import static com.jnks.iot.server.dao.model.ModelConstants.ALARM_COMMENT_COMMENT;
import static com.jnks.iot.server.dao.model.ModelConstants.ALARM_COMMENT_TYPE;

@Data
@EqualsAndHashCode(callSuper = true)
@MappedSuperclass
public abstract class AbstractAlarmCommentEntity<T extends AlarmComment> extends BaseSqlEntity<T> {

    @Column(name = ALARM_COMMENT_ALARM_ID, columnDefinition = "uuid")
    private UUID alarmId;

    @Column(name = ModelConstants.ALARM_COMMENT_USER_ID)
    private UUID userId;

    @Column(name = ALARM_COMMENT_TYPE)
    private AlarmCommentType type;

    @Convert(converter = JsonConverter.class)
    @Column(name = ALARM_COMMENT_COMMENT)
    private JsonNode comment;

    public AbstractAlarmCommentEntity() {
        super();
    }

    public AbstractAlarmCommentEntity(AlarmComment alarmComment) {
        if (alarmComment.getId() != null) {
            this.setUuid(alarmComment.getUuidId());
        }
        this.setCreatedTime(alarmComment.getCreatedTime());
        this.alarmId = alarmComment.getAlarmId().getId();
        if (alarmComment.getUserId() != null) {
            this.userId = alarmComment.getUserId().getId();
        }
        if (alarmComment.getType() != null) {
            this.type = alarmComment.getType();
        }
        this.setComment(alarmComment.getComment());
    }

    public AbstractAlarmCommentEntity(AlarmCommentEntity alarmCommentEntity) {
        this.setId(alarmCommentEntity.getId());
        this.setCreatedTime(alarmCommentEntity.getCreatedTime());
        this.userId = alarmCommentEntity.getUserId();
        this.alarmId = alarmCommentEntity.getAlarmId();
        this.type = alarmCommentEntity.getType();
        this.comment = alarmCommentEntity.getComment();
    }

    protected AlarmComment toAlarmComment() {
        AlarmComment alarmComment = new AlarmComment(new AlarmCommentId(id));
        alarmComment.setCreatedTime(createdTime);
        alarmComment.setAlarmId(new AlarmId(alarmId));
        if (userId != null) {
            alarmComment.setUserId(new UserId(userId));
        }
        alarmComment.setType(type);
        alarmComment.setComment(comment);
        return alarmComment;
    }

}
