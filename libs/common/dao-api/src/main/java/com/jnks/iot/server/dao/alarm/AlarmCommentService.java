package com.jnks.iot.server.dao.alarm;

import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.server.common.data.alarm.AlarmComment;
import com.jnks.iot.server.common.data.alarm.AlarmCommentInfo;
import com.jnks.iot.server.common.data.id.AlarmCommentId;
import com.jnks.iot.server.common.data.id.AlarmId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;

public interface AlarmCommentService {

    AlarmComment createOrUpdateAlarmComment(TenantId tenantId, AlarmComment alarmComment);

    AlarmComment saveAlarmComment(TenantId tenantId, AlarmComment alarmComment);

    PageData<AlarmCommentInfo> findAlarmComments(TenantId tenantId, AlarmId alarmId, PageLink pageLink);

    ListenableFuture<AlarmComment> findAlarmCommentByIdAsync(TenantId tenantId, AlarmCommentId alarmCommentId);

    AlarmComment findAlarmCommentById(TenantId tenantId, AlarmCommentId alarmCommentId);

}
