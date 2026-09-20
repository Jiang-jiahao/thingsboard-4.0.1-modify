package com.jnks.iot.server.dao.sql.alarm;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.alarm.EntityAlarm;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.TenantEntityDao;
import com.jnks.iot.server.dao.util.SqlDao;

@Component
@SqlDao
public class JpaEntityAlarmDao implements TenantEntityDao<EntityAlarm> {

    @Autowired
    private EntityAlarmRepository entityAlarmRepository;

    @Override
    public PageData<EntityAlarm> findAllByTenantId(TenantId tenantId, PageLink pageLink) {
        return DaoUtil.toPageData(entityAlarmRepository.findByTenantId(tenantId.getId(), DaoUtil.toPageable(pageLink, "entityId", "alarmId")));
    }

}
