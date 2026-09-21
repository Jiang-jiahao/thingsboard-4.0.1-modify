package com.jnks.iot.server.service.security.auth.oauth2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.common.data.*;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.DashboardId;
import com.jnks.iot.server.common.data.id.IdBased;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.oauth2.OAuth2Client;
import com.jnks.iot.server.common.data.oauth2.OAuth2MapperConfig;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.security.Authority;
import com.jnks.iot.server.common.data.security.UserCredentials;
import com.jnks.iot.server.dao.customer.CustomerService;
import com.jnks.iot.server.dao.dashboard.DashboardService;
import com.jnks.iot.server.dao.oauth2.OAuth2User;
import com.jnks.iot.server.dao.tenant.JnksIotTenantProfileCache;
import com.jnks.iot.server.dao.tenant.TenantService;
import com.jnks.iot.server.dao.user.UserService;
import com.jnks.iot.server.service.entitiy.tenant.JnksIotTenantService;
import com.jnks.iot.server.service.entitiy.user.JnksIotUserService;
import com.jnks.iot.server.service.install.InstallScripts;
import com.jnks.iot.server.service.security.model.SecurityUser;
import com.jnks.iot.server.service.security.model.UserPrincipal;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public abstract class AbstractOAuth2ClientMapper {
    private static final int DASHBOARDS_REQUEST_LIMIT = 10;

    @Autowired
    private UserService userService;

    @Autowired
    private BCryptPasswordEncoder passwordEncoder;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private JnksIotTenantService jnksIotTenantService;

    @Autowired
    private CustomerService customerService;

    @Autowired
    private DashboardService dashboardService;

    @Autowired
    private InstallScripts installScripts;

    @Autowired
    private JnksIotUserService jnksIotUserService;

    @Autowired
    protected JnksIotTenantProfileCache tenantProfileCache;

    @Autowired
    private ApplicationEventPublisher eventPublisher;

    private final Lock userCreationLock = new ReentrantLock();

    protected SecurityUser getOrCreateSecurityUserFromOAuth2User(OAuth2User oauth2User, OAuth2Client oAuth2Client) {

        OAuth2MapperConfig config = oAuth2Client.getMapperConfig();

        UserPrincipal principal = new UserPrincipal(UserPrincipal.Type.USER_NAME, oauth2User.getEmail());

        User user = userService.findUserByEmail(TenantId.SYS_TENANT_ID, oauth2User.getEmail());

        if (user == null && !config.isAllowUserCreation()) {
            throw new UsernameNotFoundException("User not found: " + oauth2User.getEmail());
        }

        if (user == null) {
            userCreationLock.lock();
            try {
                user = userService.findUserByEmail(TenantId.SYS_TENANT_ID, oauth2User.getEmail());
                if (user == null) {
                    user = new User();
                    if (oauth2User.getCustomerId() == null && StringUtils.isEmpty(oauth2User.getCustomerName())) {
                        user.setAuthority(Authority.TENANT_ADMIN);
                    } else {
                        user.setAuthority(Authority.CUSTOMER_USER);
                    }
                    TenantId tenantId = oauth2User.getTenantId() != null ?
                            oauth2User.getTenantId() : getTenantId(oauth2User.getTenantName());
                    user.setTenantId(tenantId);
                    CustomerId customerId = oauth2User.getCustomerId() != null ?
                            oauth2User.getCustomerId() : getCustomerId(user.getTenantId(), oauth2User.getCustomerName());
                    user.setCustomerId(customerId);
                    user.setEmail(oauth2User.getEmail());
                    user.setFirstName(oauth2User.getFirstName());
                    user.setLastName(oauth2User.getLastName());

                    ObjectNode additionalInfo = JacksonUtil.newObjectNode();

                    if (!StringUtils.isEmpty(oauth2User.getDefaultDashboardName())) {
                        Optional<DashboardId> dashboardIdOpt =
                                user.getAuthority() == Authority.TENANT_ADMIN ?
                                        getDashboardId(tenantId, oauth2User.getDefaultDashboardName())
                                        : getDashboardId(tenantId, customerId, oauth2User.getDefaultDashboardName());
                        if (dashboardIdOpt.isPresent()) {
                            additionalInfo.put("defaultDashboardFullscreen", oauth2User.isAlwaysFullScreen());
                            additionalInfo.put("defaultDashboardId", dashboardIdOpt.get().getId().toString());
                        }
                    }

                    if (oAuth2Client.getAdditionalInfo() != null &&
                            oAuth2Client.getAdditionalInfo().has("providerName")) {
                        additionalInfo.put("authProviderName", oAuth2Client.getAdditionalInfo().get("providerName").asText());
                    }

                    user.setAdditionalInfo(additionalInfo);

                    user = jnksIotUserService.save(tenantId, customerId, user, false, null, null);
                    if (config.isActivateUser()) {
                        UserCredentials userCredentials = userService.findUserCredentialsByUserId(user.getTenantId(), user.getId());
                        userService.activateUserCredentials(user.getTenantId(), userCredentials.getActivateToken(), passwordEncoder.encode(""));
                    }
                }
            } catch (Exception e) {
                log.error("Can't get or create security user from oauth2 user", e);
                throw new RuntimeException("Can't get or create security user from oauth2 user", e);
            } finally {
                userCreationLock.unlock();
            }
        }

        try {
            SecurityUser securityUser = new SecurityUser(user, true, principal);
            return (SecurityUser) new UsernamePasswordAuthenticationToken(securityUser, null, securityUser.getAuthorities()).getPrincipal();
        } catch (Exception e) {
            log.error("Can't get or create security user from oauth2 user", e);
            throw new RuntimeException("Can't get or create security user from oauth2 user", e);
        }
    }

    private TenantId getTenantId(String tenantName) throws Exception {
        List<Tenant> tenants = tenantService.findTenants(new PageLink(1, 0, tenantName)).getData();
        Tenant tenant;
        if (tenants == null || tenants.isEmpty()) {
            tenant = new Tenant();
            tenant.setTitle(tenantName);
            tenant = jnksIotTenantService.save(tenant);
        } else {
            tenant = tenants.get(0);
        }
        return tenant.getTenantId();
    }

    private CustomerId getCustomerId(TenantId tenantId, String customerName) {
        if (StringUtils.isEmpty(customerName)) {
            return null;
        }
        Optional<Customer> customerOpt = customerService.findCustomerByTenantIdAndTitle(tenantId, customerName);
        if (customerOpt.isPresent()) {
            return customerOpt.get().getId();
        } else {
            Customer customer = new Customer();
            customer.setTenantId(tenantId);
            customer.setTitle(customerName);
            return customerService.saveCustomer(customer).getId();
        }
    }

    private Optional<DashboardId> getDashboardId(TenantId tenantId, String dashboardName) {
        return Optional.ofNullable(dashboardService.findFirstDashboardInfoByTenantIdAndName(tenantId, dashboardName)).map(IdBased::getId);
    }

    private Optional<DashboardId> getDashboardId(TenantId tenantId, CustomerId customerId, String dashboardName) {
        PageData<DashboardInfo> dashboardsPage;
        PageLink pageLink = null;
        do {
            pageLink = pageLink == null ? new PageLink(DASHBOARDS_REQUEST_LIMIT) : pageLink.nextPageLink();
            dashboardsPage = dashboardService.findDashboardsByTenantIdAndCustomerId(tenantId, customerId, pageLink);
            Optional<DashboardInfo> dashboardInfoOpt = dashboardsPage.getData().stream()
                    .filter(dashboardInfo -> dashboardName.equals(dashboardInfo.getName()))
                    .findAny();
            if (dashboardInfoOpt.isPresent()) {
                return dashboardInfoOpt.map(DashboardInfo::getId);
            }
        } while (dashboardsPage.hasNext());
        return Optional.empty();
    }
}
