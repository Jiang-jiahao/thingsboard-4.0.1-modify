package com.jnks.iot.server.common.data.alarm;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.HasTenantId;
import com.jnks.iot.server.common.data.id.AlarmId;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.UserId;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EntityAlarm implements HasTenantId {

    private TenantId tenantId;
    private EntityId entityId;
    private long createdTime;
    private String alarmType;

    private CustomerId customerId;
    private UserId assigneeId;
    private AlarmId alarmId;

}
