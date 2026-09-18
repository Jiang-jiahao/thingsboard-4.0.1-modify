package org.thingsboard.monitoring.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.thingsboard.rest.client.RestClient;

import java.time.Duration;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class TbClient extends RestClient {

    @Value("${monitoring.rest.username}")
    private String username;
    @Value("${monitoring.rest.password}")
    private String password;

    public TbClient(@Value("${monitoring.rest.base_url}") String baseUrl,
                    @Value("${monitoring.rest.request_timeout_ms}") int requestTimeoutMs) {
        super(new RestTemplateBuilder()
                .setConnectTimeout(Duration.ofMillis(requestTimeoutMs))
                .setReadTimeout(Duration.ofMillis(requestTimeoutMs))
                .build(), baseUrl);
    }

    public String logIn() {
        login(username, password);
        return getToken();
    }

}
