package com.jnks.iot.server.common.data.device.profile;

import lombok.Data;
import com.jnks.iot.server.common.data.DeviceTransportType;
import com.jnks.iot.server.common.data.transport.snmp.config.SnmpCommunicationConfig;

import java.util.List;

@Data
public class SnmpDeviceProfileTransportConfiguration implements DeviceProfileTransportConfiguration {
    private Integer timeoutMs;
    private Integer retries;
    private List<SnmpCommunicationConfig> communicationConfigs;

    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.SNMP;
    }

    @Override
    public void validate() {
        if (timeoutMs == null) {
            throw new IllegalArgumentException("timeoutMs is required");
        }
        if (timeoutMs < 0) {
            throw new IllegalArgumentException("timeoutMs must be >= 0");
        }
        if (retries == null) {
            throw new IllegalArgumentException("retries is required");
        }
        if (retries < 0) {
            throw new IllegalArgumentException("retries must be >= 0");
        }
        if (communicationConfigs == null || communicationConfigs.isEmpty()) {
            throw new IllegalArgumentException(
                    "communicationConfigs must contain at least one SNMP communication configuration");
        }
        for (int i = 0; i < communicationConfigs.size(); i++) {
            SnmpCommunicationConfig config = communicationConfigs.get(i);
            if (config == null) {
                throw new IllegalArgumentException("communicationConfigs[" + i + "] must not be null");
            }
            if (!config.isValid()) {
                throw new IllegalArgumentException(
                        "communicationConfigs[" + i + "] is not valid: mappings must not be empty"
                                + " and queryingFrequencyMs (when applicable) must be greater than 0");
            }
        }
    }

}
