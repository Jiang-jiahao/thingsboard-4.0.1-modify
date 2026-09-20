package com.jnks.iot.server.dao.alarm;

import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.server.common.data.alarm.AlarmComment;
import com.jnks.iot.server.common.data.alarm.AlarmCommentInfo;
import com.jnks.iot.server.common.data.id.AlarmId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.Dao;

import java.util.UUID;

public interface AlarmCommentDao extends Dao<AlarmComment> {

    AlarmComment findAlarmCommentById(TenantId tenantId, UUID key);

    PageData<AlarmCommentInfo> findAlarmComments(TenantId tenantId, AlarmId id, PageLink pageLink);

    ListenableFuture<AlarmComment> findAlarmCommentByIdAsync(TenantId tenantId, UUID key);

}
