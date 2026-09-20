package com.jnks.iot.server.dao.device;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.core.io.Resource;
import com.jnks.iot.server.common.data.Device;

import java.net.URISyntaxException;

public interface DeviceConnectivityService {

    JsonNode findDevicePublishTelemetryCommands(String baseUrl, Device device) throws URISyntaxException;

    Resource getPemCertFile(String protocol);

    Resource createGatewayDockerComposeFile(String baseUrl, Device device) throws URISyntaxException;
}
