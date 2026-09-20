package com.jnks.iot.server.common.data.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.id.DomainId;
import com.jnks.iot.server.common.data.id.OAuth2ClientId;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class DomainOauth2Client {

    private DomainId domainId;
    private OAuth2ClientId oAuth2ClientId;

}
