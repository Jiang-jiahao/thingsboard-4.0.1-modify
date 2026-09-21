package com.jnks.iot.server.service.entitiy.alarm;

import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.alarm.Alarm;
import com.jnks.iot.server.common.data.alarm.AlarmInfo;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.UserId;

import java.util.List;
import java.util.UUID;

/**
 * 告警业务层契约：创建/更新、确认、清除、分配与删除。
 * <p>
 * 由 AlarmController 调用；实现类走告警订阅服务并写审计、系统评论与通知触发。
 */
public interface JnksIotAlarmService {

    /** 保存告警（新建或更新，并同步确认/清除/分配状态）。 */
    Alarm save(Alarm entity, User user) throws JnksIotException;

    /** 确认告警（时间戳取当前时刻）。 */
    AlarmInfo ack(Alarm alarm, User user) throws JnksIotException;

    /** 按指定时间戳确认告警。 */
    AlarmInfo ack(Alarm alarm, long ackTs, User user) throws JnksIotException;

    /** 清除告警（时间戳取当前时刻）。 */
    AlarmInfo clear(Alarm alarm, User user) throws JnksIotException;

    /** 按指定时间戳清除告警。 */
    AlarmInfo clear(Alarm alarm, long clearTs, User user) throws JnksIotException;

    /** 将告警分配给指定用户。 */
    AlarmInfo assign(Alarm alarm, UserId assigneeId, long assignTs, User user) throws JnksIotException;

    /** 取消告警分配。 */
    AlarmInfo unassign(Alarm alarm, long unassignTs, User user) throws JnksIotException;

    /** 用户删除时批量取消其名下告警的分配。 */
    void unassignDeletedUserAlarms(TenantId tenantId, UserId userId, String userTitle, List<UUID> alarms, long unassignTs);

    /** 删除告警。 */
    Boolean delete(Alarm alarm, User user);
}
