package com.jnks.iot.server.common.data.mobile.app;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.validation.NoXss;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StoreInfo {

    @NoXss
    private String appId;
    @NoXss
    private String sha256CertFingerprints;
    @NoXss
    private String storeLink;

}
