package com.jnks.iot.server.common.data.mobile.qrCodeSettings;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.server.common.data.BaseData;
import com.jnks.iot.server.common.data.HasTenantId;
import com.jnks.iot.server.common.data.id.MobileAppBundleId;
import com.jnks.iot.server.common.data.id.QrCodeSettingsId;
import com.jnks.iot.server.common.data.id.TenantId;

@Schema
@Data
@EqualsAndHashCode(callSuper = true)
public class QrCodeSettings extends BaseData<QrCodeSettingsId> implements HasTenantId {

    private static final long serialVersionUID = 2628323657987010348L;

    @Schema(description = "JSON object with Tenant Id.", accessMode = Schema.AccessMode.READ_ONLY)
    private TenantId tenantId;
    @Schema(description = "Use settings from system level", example = "true")
    private boolean useSystemSettings;
    @Schema(description = "Type of application: true means use default JnksIOT app", example = "true")
    private boolean useDefaultApp;
    @Schema(description = "Mobile app bundle.")
    private MobileAppBundleId mobileAppBundleId;
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = "QR code config configuration.")
    @Valid
    @NotNull
    private QRCodeConfig qrCodeConfig;
    @Schema(description = "Indicates if google play link is available", example = "true")
    private boolean androidEnabled;
    @Schema(description = "Indicates if apple store link is available", example = "true")
    private boolean iosEnabled;
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private String googlePlayLink;
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private String appStoreLink;

    public QrCodeSettings() {
    }

    public QrCodeSettings(QrCodeSettingsId id) {
        super(id);
    }

}
