package com.jnks.iot.server.service.entitiy.mobile;

import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.mobile.app.MobileApp;

/**
 * 移动应用业务层契约：保存与删除。
 */
public interface JnksIotMobileAppService {

    /** 保存移动应用。 */
    MobileApp save(MobileApp mobileApp, User user) throws Exception;

    /** 删除移动应用。 */
    void delete(MobileApp mobileApp, User user);

}
