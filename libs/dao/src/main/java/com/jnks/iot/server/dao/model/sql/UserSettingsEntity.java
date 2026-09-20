package com.jnks.iot.server.dao.model.sql;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcType;
import org.hibernate.dialect.PostgreSQLJsonPGObjectJsonbType;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.settings.UserSettings;
import com.jnks.iot.server.common.data.settings.UserSettingsCompositeKey;
import com.jnks.iot.server.common.data.settings.UserSettingsType;
import com.jnks.iot.server.dao.model.ModelConstants;
import com.jnks.iot.server.dao.model.ToData;
import com.jnks.iot.server.dao.util.mapping.JsonConverter;

import java.util.UUID;

@Data
@NoArgsConstructor
@Entity
@Table(name = ModelConstants.USER_SETTINGS_TABLE_NAME)
@IdClass(UserSettingsCompositeKey.class)
public class UserSettingsEntity implements ToData<UserSettings> {

    @Id
    @Column(name = ModelConstants.USER_SETTINGS_USER_ID_PROPERTY)
    private UUID userId;
    @Id
    @Column(name = ModelConstants.USER_SETTINGS_TYPE_PROPERTY)
    private String type;
    @Convert(converter = JsonConverter.class)
    @JdbcType(PostgreSQLJsonPGObjectJsonbType.class)
    @Column(name = ModelConstants.USER_SETTINGS_SETTINGS, columnDefinition = "jsonb")
    private JsonNode settings;

    public UserSettingsEntity(UserSettings userSettings) {
        this.userId = userSettings.getUserId().getId();
        this.type = userSettings.getType().name();
        if (userSettings.getSettings() != null) {
            this.settings = userSettings.getSettings();
        }
    }

    @Override
    public UserSettings toData() {
        UserSettings userSettings = new UserSettings();
        userSettings.setUserId(new UserId(userId));
        userSettings.setType(UserSettingsType.valueOf(type));
        if (settings != null) {
            userSettings.setSettings(settings);
        }
        return userSettings;
    }

}
