package com.jnks.iot.server.service.entitiy.alarm;

import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.alarm.Alarm;
import com.jnks.iot.server.common.data.alarm.AlarmComment;
import com.jnks.iot.server.common.data.exception.JnksIotException;

public interface TbAlarmCommentService {

    AlarmComment saveAlarmComment(Alarm alarm, AlarmComment alarmComment, User user) throws JnksIotException;

    void deleteAlarmComment(Alarm alarm, AlarmComment alarmComment, User user) throws JnksIotException;
}
