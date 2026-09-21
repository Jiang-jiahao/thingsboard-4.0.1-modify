package com.jnks.iot.server.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;
import com.jnks.iot.server.controller.plugin.JnksIotWebSocketHandler;
import com.jnks.iot.server.service.security.auth.constants.WebSocketConstants;

@Configuration
@EnableWebSocket
@RequiredArgsConstructor
@Slf4j
public class WebSocketConfiguration implements WebSocketConfigurer {


    private final WebSocketHandler wsHandler;

    @Bean
    public ServletServerContainerFactoryBean createWebSocketContainer() {
        ServletServerContainerFactoryBean container = new ServletServerContainerFactoryBean();
        container.setMaxTextMessageBufferSize(32768);
        container.setMaxBinaryMessageBufferSize(32768);
        return container;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        if (!(wsHandler instanceof JnksIotWebSocketHandler)) {
            log.error("JnksIotWebSocketHandler expected but [{}] provided", wsHandler);
            throw new RuntimeException("JnksIotWebSocketHandler expected but " + wsHandler + " provided");
        }
        registry.addHandler(wsHandler, WebSocketConstants.WS_API_MAPPING).setAllowedOriginPatterns("*");
    }

}
