package org.thingsboard.server.common.data.settings;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.ToString;
import org.thingsboard.server.common.data.id.UserId;
import org.thingsboard.server.common.data.validation.Length;
import org.thingsboard.server.common.data.validation.NoXss;

import java.io.Serializable;

import static org.thingsboard.server.common.data.BaseDataWithAdditionalInfo.getJson;
import static org.thingsboard.server.common.data.BaseDataWithAdditionalInfo.setJson;

@Schema
@Data
public class UserSettings implements Serializable {

    private static final long serialVersionUID = 2628320657987010348L;

    @Schema(description = "JSON object with User id.", accessMode = Schema.AccessMode.READ_ONLY)
    private UserId userId;

    @Schema(description = "Type of the settings.")
    @NoXss
    @Length(fieldName = "type", max = 50)
    private UserSettingsType type;

    @Schema(description = "JSON object with user settings.",implementation = com.fasterxml.jackson.databind.JsonNode.class)
    @NoXss
    @Length(fieldName = "settings", max = 100000)
    private transient JsonNode settings;

    @JsonIgnore
    @ToString.Exclude
    private byte[] settingsBytes;

    public JsonNode getSettings() {
        return getJson(() -> settings, () -> settingsBytes);
    }

    public void setSettings(JsonNode settings) {
        setJson(settings, json -> this.settings = json, bytes -> this.settingsBytes = bytes);
    }
}
