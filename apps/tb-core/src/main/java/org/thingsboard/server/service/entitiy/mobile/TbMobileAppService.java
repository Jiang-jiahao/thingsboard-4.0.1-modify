package org.thingsboard.server.service.entitiy.mobile;

import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.mobile.app.MobileApp;

/**
 * 移动应用业务层契约：保存与删除。
 */
public interface TbMobileAppService {

    /** 保存移动应用。 */
    MobileApp save(MobileApp mobileApp, User user) throws Exception;

    /** 删除移动应用。 */
    void delete(MobileApp mobileApp, User user);

}
