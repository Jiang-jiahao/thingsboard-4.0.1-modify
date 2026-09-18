package org.thingsboard.server.service.entitiy.domain;

import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.domain.Domain;
import org.thingsboard.server.common.data.id.OAuth2ClientId;

import java.util.List;

/**
 * 登录域名（Domain）业务层契约：保存、绑定 OAuth2 客户端与删除。
 */
public interface TbDomainService {

    /** 保存域名并可同时绑定 OAuth2 客户端。 */
    Domain save(Domain domain, List<OAuth2ClientId> oAuth2Clients, User user) throws Exception;

    /** 更新域名绑定的 OAuth2 客户端列表。 */
    void updateOauth2Clients(Domain domain, List<OAuth2ClientId> oAuth2ClientIds, User user);

    /** 删除域名。 */
    void delete(Domain domain, User user);

}
