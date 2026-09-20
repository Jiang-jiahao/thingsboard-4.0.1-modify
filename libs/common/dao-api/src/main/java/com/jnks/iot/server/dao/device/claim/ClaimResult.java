package com.jnks.iot.server.dao.device.claim;


import lombok.AllArgsConstructor;
import lombok.Data;
import com.jnks.iot.server.common.data.Device;

@AllArgsConstructor
@Data
public class ClaimResult {

    private Device device;
    private ClaimResponse response;

}
