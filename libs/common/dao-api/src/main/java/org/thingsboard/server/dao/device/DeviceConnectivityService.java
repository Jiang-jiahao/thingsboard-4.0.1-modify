package org.thingsboard.server.dao.device;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.core.io.Resource;
import org.thingsboard.server.common.data.Device;

import java.net.URISyntaxException;

public interface DeviceConnectivityService {

    JsonNode findDevicePublishTelemetryCommands(String baseUrl, Device device) throws URISyntaxException;

    Resource getPemCertFile(String protocol);

    Resource createGatewayDockerComposeFile(String baseUrl, Device device) throws URISyntaxException;
}
