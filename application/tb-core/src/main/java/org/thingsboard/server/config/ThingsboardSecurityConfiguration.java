/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.security.SecurityProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.DefaultAuthenticationEventPublisher;
import org.springframework.security.config.annotation.ObjectPostProcessor;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.annotation.web.configurers.RequestCacheConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.client.web.OAuth2AuthorizationRequestResolver;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;
import org.springframework.security.web.authentication.AuthenticationSuccessHandler;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.web.header.writers.StaticHeadersWriter;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.filter.CorsFilter;
import org.springframework.web.filter.ShallowEtagHeaderFilter;
import org.thingsboard.server.dao.oauth2.OAuth2Configuration;
import org.thingsboard.server.exception.ThingsboardErrorResponseHandler;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.security.auth.constants.ThingsboardSecurityConstants;
import org.thingsboard.server.service.security.auth.jwt.JwtAuthenticationProvider;
import org.thingsboard.server.service.security.auth.jwt.JwtTokenAuthenticationProcessingFilter;
import org.thingsboard.server.service.security.auth.jwt.RefreshTokenAuthenticationProvider;
import org.thingsboard.server.service.security.auth.jwt.RefreshTokenProcessingFilter;
import org.thingsboard.server.service.security.auth.jwt.SkipPathRequestMatcher;
import org.thingsboard.server.service.security.auth.jwt.extractor.TokenExtractor;
import org.thingsboard.server.service.security.auth.oauth2.HttpCookieOAuth2AuthorizationRequestRepository;
import org.thingsboard.server.service.security.auth.rest.RestAuthenticationProvider;
import org.thingsboard.server.service.security.auth.rest.RestLoginProcessingFilter;
import org.thingsboard.server.service.security.auth.rest.RestPublicLoginProcessingFilter;
import org.thingsboard.server.transport.http.config.PayloadSizeFilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Configuration
@EnableWebSecurity
@EnableMethodSecurity
@Order(SecurityProperties.BASIC_AUTH_ORDER)
@TbCoreComponent
public class ThingsboardSecurityConfiguration {


    // HTTP请求最大负载大小配置
    @Value("${server.http.max_payload_size:/api/image*/**=52428800;/api/resource/**=52428800;/api/**=16777216}")
    private String maxPayloadSizeConfig;

    @Autowired
    private ThingsboardErrorResponseHandler restAccessDeniedHandler;

    // oauth2登录成功处理器
    @Autowired(required = false)
    @Qualifier("oauth2AuthenticationSuccessHandler")
    private AuthenticationSuccessHandler oauth2AuthenticationSuccessHandler;

    // oauth2登录失败处理器
    @Autowired(required = false)
    @Qualifier("oauth2AuthenticationFailureHandler")
    private AuthenticationFailureHandler oauth2AuthenticationFailureHandler;

    // OAuth2授权请求存储库
    @Autowired(required = false)
    private HttpCookieOAuth2AuthorizationRequestRepository httpCookieOAuth2AuthorizationRequestRepository;

    // 默认的登录成功处理器
    @Autowired
    @Qualifier("defaultAuthenticationSuccessHandler")
    private AuthenticationSuccessHandler successHandler;

    // 默认的登录失败处理器
    @Autowired
    @Qualifier("defaultAuthenticationFailureHandler")
    private AuthenticationFailureHandler failureHandler;

    // REST认证提供器
    @Autowired
    private RestAuthenticationProvider restAuthenticationProvider;

    // JWT认证提供器
    @Autowired
    private JwtAuthenticationProvider jwtAuthenticationProvider;

    // 刷新Token认证提供器
    @Autowired
    private RefreshTokenAuthenticationProvider refreshTokenAuthenticationProvider;

    // OAuth2配置（可选）
    @Autowired(required = false)
    OAuth2Configuration oauth2Configuration;

    // JWT头Token提取器
    @Autowired
    @Qualifier("jwtHeaderTokenExtractor")
    private TokenExtractor jwtHeaderTokenExtractor;

    // Spring Security认证管理器，里面管理了很多认证提供器。
    @Autowired
    private AuthenticationManager authenticationManager;

    // 请求速率限制过滤器
    @Autowired
    private RateLimitProcessingFilter rateLimitProcessingFilter;

    /**
     * 配置负载大小过滤器
     * 用于限制HTTP请求的最大负载大小
     */
    @Bean
    protected PayloadSizeFilter payloadSizeFilter() {
        return new PayloadSizeFilter(maxPayloadSizeConfig);
    }

    /**
     * 配置ETag过滤器
     * 用于静态资源的缓存优化，减少带宽消耗
     */
    @Bean
    protected FilterRegistrationBean<ShallowEtagHeaderFilter> buildEtagFilter() throws Exception {
        ShallowEtagHeaderFilter etagFilter = new ShallowEtagHeaderFilter();
        // 启用弱ETag
        etagFilter.setWriteWeakETag(true);
        FilterRegistrationBean<ShallowEtagHeaderFilter> filterRegistrationBean
                = new FilterRegistrationBean<>(etagFilter);
        // 指定哪些 URL 路径会被 ShallowEtagHeaderFilter 这个过滤器拦截并处理
        filterRegistrationBean.addUrlPatterns("*.js", "*.css", "*.ico", "/assets/*", "/static/*");
        filterRegistrationBean.setName("etagFilter");
        return filterRegistrationBean;
    }

    /**
     * 配置REST登录处理过滤器
     * @return REST登录处理过滤器
     * @throws Exception ex
     */
    @Bean
    protected RestLoginProcessingFilter buildRestLoginProcessingFilter() throws Exception {
        RestLoginProcessingFilter filter = new RestLoginProcessingFilter(ThingsboardSecurityConstants.FORM_BASED_LOGIN_ENTRY_POINT, successHandler, failureHandler);
        filter.setAuthenticationManager(this.authenticationManager);
        return filter;
    }


    /**
     * 配置公共REST登录处理过滤器
     * @return 公共REST登录处理过滤器
     * @throws Exception ex
     */
    @Bean
    protected RestPublicLoginProcessingFilter buildRestPublicLoginProcessingFilter() throws Exception {
        RestPublicLoginProcessingFilter filter = new RestPublicLoginProcessingFilter(ThingsboardSecurityConstants.PUBLIC_LOGIN_ENTRY_POINT, successHandler, failureHandler);
        filter.setAuthenticationManager(this.authenticationManager);
        return filter;
    }

    /**
     * 配置JWT Token认证处理过滤器，验证JWT Token的有效性
     * @return Token认证处理过滤器
     * @throws Exception ex
     */
    protected JwtTokenAuthenticationProcessingFilter buildJwtTokenAuthenticationProcessingFilter() throws Exception {
        // 配置无需jwt认证的url
        List<String> pathsToSkip = new ArrayList<>(Arrays.asList(ThingsboardSecurityConstants.NON_TOKEN_BASED_AUTH_ENTRY_POINTS));
        pathsToSkip.addAll(Arrays.asList(ThingsboardSecurityConstants.WS_ENTRY_POINT, ThingsboardSecurityConstants.TOKEN_REFRESH_ENTRY_POINT, ThingsboardSecurityConstants.FORM_BASED_LOGIN_ENTRY_POINT,
                ThingsboardSecurityConstants.PUBLIC_LOGIN_ENTRY_POINT, ThingsboardSecurityConstants.DEVICE_API_ENTRY_POINT, ThingsboardSecurityConstants.MAIL_OAUTH2_PROCESSING_ENTRY_POINT,
                ThingsboardSecurityConstants.DEVICE_CONNECTIVITY_CERTIFICATE_DOWNLOAD_ENTRY_POINT));
        // 创建路径请求匹配器，自定义路径匹配的规则
        SkipPathRequestMatcher matcher = new SkipPathRequestMatcher(pathsToSkip, ThingsboardSecurityConstants.TOKEN_BASED_AUTH_ENTRY_POINT);
        JwtTokenAuthenticationProcessingFilter filter
                = new JwtTokenAuthenticationProcessingFilter(failureHandler, jwtHeaderTokenExtractor, matcher);
        filter.setAuthenticationManager(this.authenticationManager);
        return filter;
    }

    /**
     * 配置刷新Token处理过滤器，处理Token刷新请求
     * @return 刷新Token处理过滤器
     * @throws Exception ex
     */
    @Bean
    protected RefreshTokenProcessingFilter buildRefreshTokenProcessingFilter() throws Exception {
        RefreshTokenProcessingFilter filter = new RefreshTokenProcessingFilter(ThingsboardSecurityConstants.TOKEN_REFRESH_ENTRY_POINT, successHandler, failureHandler);
        filter.setAuthenticationManager(this.authenticationManager);
        return filter;
    }

    /**
     * 配置认证管理器
     * 注册各种认证提供器
     */
    @Bean
    public AuthenticationManager authenticationManager(ObjectPostProcessor<Object> objectPostProcessor) throws Exception {
        DefaultAuthenticationEventPublisher eventPublisher = objectPostProcessor
                .postProcess(new DefaultAuthenticationEventPublisher());
        var auth = new AuthenticationManagerBuilder(objectPostProcessor);
        auth.authenticationEventPublisher(eventPublisher);
        auth.authenticationProvider(restAuthenticationProvider);
        auth.authenticationProvider(jwtAuthenticationProvider);
        auth.authenticationProvider(refreshTokenAuthenticationProvider);
        return auth.build();
    }

    @Autowired
    private OAuth2AuthorizationRequestResolver oAuth2AuthorizationRequestResolver;

    @Bean
    @Order(0)
    SecurityFilterChain resources(HttpSecurity http) throws Exception {
        http
                .securityMatchers(matchers -> matchers
                        .requestMatchers("/*.js", "/*.css", "/*.ico", "/assets/**", "/static/**"))
                .headers(header -> header
                        .defaultsDisabled()
                        .addHeaderWriter(new StaticHeadersWriter(HttpHeaders.CACHE_CONTROL, "max-age=0, public")))
                .authorizeHttpRequests((authorize) -> authorize.anyRequest().permitAll())
                .requestCache(RequestCacheConfigurer::disable)
                .securityContext(AbstractHttpConfigurer::disable)
                .sessionManagement(AbstractHttpConfigurer::disable);
        return http.build();
    }

    @Bean
    SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http.headers(headers -> headers
                        .cacheControl(config -> {})
                        .frameOptions(config -> {}).disable())
                .cors(cors -> {})
                .csrf(AbstractHttpConfigurer::disable)
                .exceptionHandling(config -> {})
                .sessionManagement(config -> config.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                // 这里的白名单配置有点重复，早期没有authorizeHttpRequests，白名单全靠 SkipPathRequestMatcher
                // 性能上：SkipPathRequestMatcher 提前短路，反而更优。
                .authorizeHttpRequests(config -> config
                        .requestMatchers(ThingsboardSecurityConstants.NON_TOKEN_BASED_AUTH_ENTRY_POINTS).permitAll() // static resources, user activation and password reset end-points (webjars included)
                        .requestMatchers(
                                ThingsboardSecurityConstants.FORM_BASED_LOGIN_ENTRY_POINT, // Login end-point
                                ThingsboardSecurityConstants.PUBLIC_LOGIN_ENTRY_POINT, // Public login end-point
                                ThingsboardSecurityConstants.TOKEN_REFRESH_ENTRY_POINT, // Token refresh end-point
                                ThingsboardSecurityConstants.MAIL_OAUTH2_PROCESSING_ENTRY_POINT, // Mail oauth2 code processing url
                                ThingsboardSecurityConstants.DEVICE_CONNECTIVITY_CERTIFICATE_DOWNLOAD_ENTRY_POINT, // Device connectivity certificate (public)
                                ThingsboardSecurityConstants.WS_ENTRY_POINT).permitAll() // Protected WebSocket API End-points
                        .requestMatchers(ThingsboardSecurityConstants.TOKEN_BASED_AUTH_ENTRY_POINT).authenticated() // Protected API End-points
                        .anyRequest().permitAll())
                .exceptionHandling(config -> config.accessDeniedHandler(restAccessDeniedHandler))
                // 设置过滤器到对应的UsernamePasswordAuthenticationFilter过滤器之前
                .addFilterBefore(buildRestLoginProcessingFilter(), UsernamePasswordAuthenticationFilter.class)
                .addFilterBefore(buildRestPublicLoginProcessingFilter(), UsernamePasswordAuthenticationFilter.class)
                .addFilterBefore(buildJwtTokenAuthenticationProcessingFilter(), UsernamePasswordAuthenticationFilter.class)
                .addFilterBefore(buildRefreshTokenProcessingFilter(), UsernamePasswordAuthenticationFilter.class)
                .addFilterBefore(payloadSizeFilter(), UsernamePasswordAuthenticationFilter.class)
                .addFilterAfter(rateLimitProcessingFilter, UsernamePasswordAuthenticationFilter.class);
        if (oauth2Configuration != null) {
            http.oauth2Login(login -> login
                    .authorizationEndpoint(config -> config
                            .authorizationRequestRepository(httpCookieOAuth2AuthorizationRequestRepository)
                            .authorizationRequestResolver(oAuth2AuthorizationRequestResolver))
                    .loginPage("/oauth2Login")
                    .loginProcessingUrl(oauth2Configuration.getLoginProcessingUrl())
                    .successHandler(oauth2AuthenticationSuccessHandler)
                    .failureHandler(oauth2AuthenticationFailureHandler));
        }
        return http.build();
    }

    @Bean
    @ConditionalOnMissingBean(CorsFilter.class)
    public CorsFilter corsFilter(@Autowired MvcCorsProperties mvcCorsProperties) {
        if (mvcCorsProperties.getMappings().isEmpty()) {
            return new CorsFilter(new UrlBasedCorsConfigurationSource());
        } else {
            UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
            source.setCorsConfigurations(mvcCorsProperties.getMappings());
            return new CorsFilter(source);
        }
    }
}
