package org.thingsboard.server.common.data.limit;

import lombok.Getter;
import org.thingsboard.server.common.data.tenant.profile.DefaultTenantProfileConfiguration;

import java.util.Optional;
import java.util.function.Function;

public enum LimitedApi {

    ENTITY_EXPORT(DefaultTenantProfileConfiguration::getTenantEntityExportRateLimit, "entity version creation", true),
    ENTITY_IMPORT(DefaultTenantProfileConfiguration::getTenantEntityImportRateLimit, "entity version load", true),
    NOTIFICATION_REQUESTS(DefaultTenantProfileConfiguration::getTenantNotificationRequestsRateLimit, "notification requests", true),
    NOTIFICATION_REQUESTS_PER_RULE(DefaultTenantProfileConfiguration::getTenantNotificationRequestsPerRuleRateLimit, "notification requests per rule", false),
    REST_REQUESTS_PER_TENANT(DefaultTenantProfileConfiguration::getTenantServerRestLimitsConfiguration, "REST API requests", true),
    REST_REQUESTS_PER_CUSTOMER(DefaultTenantProfileConfiguration::getCustomerServerRestLimitsConfiguration, "REST API requests per customer", false),
    WS_UPDATES_PER_SESSION(DefaultTenantProfileConfiguration::getWsUpdatesPerSessionRateLimit, "WS updates per session", true),
    CASSANDRA_QUERIES(DefaultTenantProfileConfiguration::getCassandraQueryTenantRateLimitsConfiguration, "Cassandra queries", true),
    PASSWORD_RESET(false, true),
    TWO_FA_VERIFICATION_CODE_SEND(false, true),
    TWO_FA_VERIFICATION_CODE_CHECK(false, true),
    TRANSPORT_MESSAGES_PER_TENANT("transport messages", true),
    TRANSPORT_MESSAGES_PER_DEVICE("transport messages per device", false),
    TRANSPORT_MESSAGES_PER_GATEWAY("transport messages per gateway", false),
    TRANSPORT_MESSAGES_PER_GATEWAY_DEVICE("transport messages per gateway device", false),
    EMAILS("emails sending", true),
    WS_SUBSCRIPTIONS("WS subscriptions", false),
    CALCULATED_FIELD_DEBUG_EVENTS("calculated field debug events", true);

    private Function<DefaultTenantProfileConfiguration, String> configExtractor;
    @Getter
    private final boolean perTenant;
    @Getter
    private boolean refillRateLimitIntervally;
    @Getter
    private String label;

    LimitedApi(Function<DefaultTenantProfileConfiguration, String> configExtractor, String label, boolean perTenant) {
        this.configExtractor = configExtractor;
        this.label = label;
        this.perTenant = perTenant;
    }

    LimitedApi(boolean perTenant, boolean refillRateLimitIntervally) {
        this.perTenant = perTenant;
        this.refillRateLimitIntervally = refillRateLimitIntervally;
    }

    LimitedApi(String label, boolean perTenant) {
        this.label = label;
        this.perTenant = perTenant;
    }

    public String getLimitConfig(DefaultTenantProfileConfiguration profileConfiguration) {
        return Optional.ofNullable(configExtractor)
                .map(extractor -> extractor.apply(profileConfiguration))
                .orElse(null);
    }

}
