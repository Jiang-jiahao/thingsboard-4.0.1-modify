package com.jnks.iot.server.service.query;

import org.springframework.http.ResponseEntity;
import org.springframework.web.context.request.async.DeferredResult;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.query.*;
import com.jnks.iot.server.service.security.model.SecurityUser;

/**
 * Core 侧实体查询门面。
 * <p>
 * 供 REST / WebSocket 按过滤器统计、分页查询实体与告警，并解析当前用户上下文中的动态过滤值。
 * 具体落库由 DAO {@code EntityService} / {@code AlarmService} 完成。
 *
 * @see DefaultEntityQueryService
 */
public interface EntityQueryService {

    /**
     * 按实体计数查询统计匹配条数。
     */
    long countEntitiesByQuery(SecurityUser securityUser, EntityCountQuery query);

    /**
     * 按实体数据查询分页返回实体及其最新字段/遥测。
     */
    PageData<EntityData> findEntityDataByQuery(SecurityUser securityUser, EntityDataQuery query);

    /**
     * 按告警数据查询分页返回告警，并合并实体最新值。
     */
    PageData<AlarmData> findAlarmDataByQuery(SecurityUser securityUser, AlarmDataQuery query);

    /**
     * 按告警计数查询统计匹配条数。
     */
    long countAlarmsByQuery(SecurityUser securityUser, AlarmCountQuery query);

    /**
     * 异步汇总查询命中实体上的时序键与属性键集合。
     */
    DeferredResult<ResponseEntity> getKeysByQuery(SecurityUser securityUser, TenantId tenantId, EntityDataQuery query,
                                                  boolean isTimeseries, boolean isAttributes, String attributesScope);

}
