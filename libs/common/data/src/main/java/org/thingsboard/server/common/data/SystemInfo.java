package org.thingsboard.server.common.data;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.util.List;

@Data
public class SystemInfo {
    @Schema(description = "Is monolith.")
    private boolean isMonolith;
    @Schema(description = "System data.")
    private List<SystemInfoData> systemData;
}
