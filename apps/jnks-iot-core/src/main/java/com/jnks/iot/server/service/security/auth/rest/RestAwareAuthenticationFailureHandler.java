package com.jnks.iot.server.service.security.auth.rest;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.exception.JnksIotErrorResponseHandler;

import java.io.IOException;

@Component(value = "defaultAuthenticationFailureHandler")
public class RestAwareAuthenticationFailureHandler implements AuthenticationFailureHandler {

    private final JnksIotErrorResponseHandler errorResponseHandler;

    @Autowired
    public RestAwareAuthenticationFailureHandler(JnksIotErrorResponseHandler errorResponseHandler) {
        this.errorResponseHandler = errorResponseHandler;
    }

    @Override
    public void onAuthenticationFailure(HttpServletRequest request, HttpServletResponse response,
                                        AuthenticationException e) throws IOException, ServletException {
        errorResponseHandler.handle(e, response);
    }
}
