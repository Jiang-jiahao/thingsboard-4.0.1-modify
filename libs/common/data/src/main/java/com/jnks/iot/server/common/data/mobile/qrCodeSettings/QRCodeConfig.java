package com.jnks.iot.server.common.data.mobile.qrCodeSettings;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.mobile.qrCodeSettings.BadgePosition;
import com.jnks.iot.server.common.data.validation.NoXss;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class QRCodeConfig {

    private boolean showOnHomePage;
    private boolean badgeEnabled;
    private boolean qrCodeLabelEnabled;
    private BadgePosition badgePosition;
    @NoXss
    private String qrCodeLabel;

}
