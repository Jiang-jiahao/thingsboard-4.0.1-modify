package com.jnks.iot.server.dao.model.sql;

import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.server.common.data.alarm.AlarmComment;
import com.jnks.iot.server.common.data.alarm.AlarmCommentInfo;

import static com.jnks.iot.server.dao.model.ModelConstants.ALARM_COMMENT_TABLE_NAME;

@Data
@EqualsAndHashCode(callSuper = true)
@Entity
@Table(name = ALARM_COMMENT_TABLE_NAME)
public class AlarmCommentEntity extends AbstractAlarmCommentEntity<AlarmComment> {

    public AlarmCommentEntity() {
        super();
    }

    public AlarmCommentEntity(AlarmCommentInfo alarmCommentInfo) {
        super(alarmCommentInfo);
    }

    public AlarmCommentEntity(AlarmComment alarmComment) {
        super(alarmComment);
    }

    @Override
    public AlarmComment toData() {
        return super.toAlarmComment();
    }

}
