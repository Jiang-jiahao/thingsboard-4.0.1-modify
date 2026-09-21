package com.jnks.iot.server.service.entitiy.cf;

import com.jnks.iot.server.common.data.cf.CalculatedField;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.CalculatedFieldId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.service.security.model.SecurityUser;

/**
 * 计算字段业务层契约：保存、查询与删除。
 * <p>
 * 由 CalculatedFieldController 调用；实现类委托 DAO 并写审计日志。
 */
public interface JnksIotCalculatedFieldService {

    /** 保存计算字段。 */
    CalculatedField save(CalculatedField calculatedField, SecurityUser user) throws JnksIotException;

    /** 按 ID 查询计算字段。 */
    CalculatedField findById(CalculatedFieldId calculatedFieldId, SecurityUser user);

    /** 分页查询某实体上的计算字段。 */
    PageData<CalculatedField> findAllByTenantIdAndEntityId(EntityId entityId, SecurityUser user, PageLink pageLink);

    /** 删除计算字段。 */
    void delete(CalculatedField calculatedField, SecurityUser user);

}
