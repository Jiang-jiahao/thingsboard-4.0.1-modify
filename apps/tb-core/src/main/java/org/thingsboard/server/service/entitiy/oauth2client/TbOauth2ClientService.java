package org.thingsboard.server.service.entitiy.oauth2client;

import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.oauth2.OAuth2Client;

/**
 * OAuth2 客户端业务层契约：保存与删除。
 */
public interface TbOauth2ClientService {

    /** 保存 OAuth2 客户端配置。 */
    OAuth2Client save(OAuth2Client oAuth2Client, User user) throws Exception;

    /** 删除 OAuth2 客户端配置。 */
    void delete(OAuth2Client oAuth2Client, User user);

}
