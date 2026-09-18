package org.thingsboard.server.service.entitiy.mobile;

import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.id.OAuth2ClientId;
import org.thingsboard.server.common.data.mobile.bundle.MobileAppBundle;

import java.util.List;

/**
 * 移动应用包业务层契约：保存、绑定 OAuth2 客户端与删除。
 */
public interface TbMobileAppBundleService {

    /** 保存移动应用包并可同时绑定 OAuth2 客户端。 */
    MobileAppBundle save(MobileAppBundle mobileAppBundle, List<OAuth2ClientId> oauth2Clients, User user) throws Exception;

    /** 更新移动应用包绑定的 OAuth2 客户端列表。 */
    void updateOauth2Clients(MobileAppBundle mobileAppBundle, List<OAuth2ClientId> oAuth2ClientIds, User user);

    /** 删除移动应用包。 */
    void delete(MobileAppBundle mobileAppBundle, User user);

}
