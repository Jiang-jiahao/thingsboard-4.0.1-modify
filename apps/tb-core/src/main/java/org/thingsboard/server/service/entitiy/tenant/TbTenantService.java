package org.thingsboard.server.service.entitiy.tenant;

import org.thingsboard.server.common.data.Tenant;

/**
 * 租户业务层契约：保存与删除。
 * <p>
 * 由 TenantController 调用；实现类会初始化默认规则链/仪表板、刷新配置缓存并调整队列。
 */
public interface TbTenantService {

    /** 保存租户；新建时安装默认规则链与仪表板。 */
    Tenant save(Tenant tenant) throws Exception;

    /** 删除租户及其版本控制设置。 */
    void delete(Tenant tenant) throws Exception;

}
