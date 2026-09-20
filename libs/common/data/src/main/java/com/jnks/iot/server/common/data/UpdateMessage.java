package com.jnks.iot.server.common.data;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.io.Serializable;

@Schema
@Data
public class UpdateMessage implements Serializable {

    @Schema(description = "'True' if new platform update is available.")
    private final boolean updateAvailable;
    @Schema(description = "Current JnksIOT version.")
    private final String currentVersion;
    @Schema(description = "Latest JnksIOT version.")
    private final String latestVersion;
    @Schema(description = "Upgrade instructions URL.")
    private final String upgradeInstructionsUrl;
    @Schema(description = "Current JnksIOT version release notes URL.")
    private final String currentVersionReleaseNotesUrl;
    @Schema(description = "Latest JnksIOT version release notes URL.")
    private final String latestVersionReleaseNotesUrl;

}
