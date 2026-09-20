package com.jnks.iot.server.dao.oauth2;

import com.jnks.iot.server.common.data.id.OAuth2ClientRegistrationTemplateId;
import com.jnks.iot.server.common.data.oauth2.OAuth2ClientRegistrationTemplate;

import java.util.List;
import java.util.Optional;

public interface OAuth2ConfigTemplateService {
    OAuth2ClientRegistrationTemplate saveClientRegistrationTemplate(OAuth2ClientRegistrationTemplate clientRegistrationTemplate);

    Optional<OAuth2ClientRegistrationTemplate> findClientRegistrationTemplateByProviderId(String providerId);

    OAuth2ClientRegistrationTemplate findClientRegistrationTemplateById(OAuth2ClientRegistrationTemplateId templateId);

    List<OAuth2ClientRegistrationTemplate> findAllClientRegistrationTemplates();

    void deleteClientRegistrationTemplateById(OAuth2ClientRegistrationTemplateId templateId);
}
