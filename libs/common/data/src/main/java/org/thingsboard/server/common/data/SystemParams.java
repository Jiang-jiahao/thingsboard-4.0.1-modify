package org.thingsboard.server.common.data;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;

import java.util.List;

@Data
public class SystemParams {
    boolean userTokenAccessEnabled;
    List<String> allowedDashboardIds;
    boolean hasRepository;
    boolean tbelEnabled;
    boolean persistDeviceStateToTelemetry;
    JsonNode userSettings;
    long maxDatapointsLimit;
    long maxResourceSize;
    boolean mobileQrEnabled;
    int maxDebugModeDurationMinutes;
    String ruleChainDebugPerTenantLimitsConfiguration;
    String calculatedFieldDebugPerTenantLimitsConfiguration;
    long maxArgumentsPerCF;
    long maxDataPointsPerRollingArg;
}
